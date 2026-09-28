package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrInvalidOptions is wrapped by the error New returns for options it
	// refuses.
	ErrInvalidOptions = errors.New("invalid supervisor options")
	// ErrAlreadyRun is returned by a second call to Engine.Run.
	ErrAlreadyRun = errors.New("the supervisor has already run")
	// errUnknownSignal is reported for a signal that is not a syscall.Signal.
	errUnknownSignal = errors.New("not a system signal")
)

// Source decides what the engine runs and whether it runs it again. Its
// methods are called from the goroutine running Engine.Run.
type Source interface {
	// Next returns the Spec for the next start. It is called before every
	// start, including the first. An error ends Run.
	Next(ctx context.Context) (Spec, error)
	// Restart reports whether to start the child again after it exited on
	// its own. It is not called when the exit followed a stop or a failed
	// check. An error ends Run.
	Restart(ctx context.Context, exit Exit) (bool, error)
}

// Options configure an Engine. A zero field means "none", except for Start.
type Options struct {
	// PidFile is the supervisor's own pid file.
	PidFile string
	// ChildPidFile is the child's pid file.
	ChildPidFile string
	// StopSignals are the received signals that stop the child and end Run.
	StopSignals []syscall.Signal
	// ReloadSignal is the received signal that runs OnReload before it is
	// forwarded. It is set together with OnReload.
	ReloadSignal syscall.Signal
	// OnReload runs on ReloadSignal, for example to reopen a log file.
	OnReload func() error
	// RestartDelay is the pause between an exit and the next start.
	RestartDelay time.Duration
	// CheckPeriod is how often Check runs while a child is running. It is set
	// together with Check.
	CheckPeriod time.Duration
	// Check is the periodic check. It runs on a goroutine of its own; an
	// error kills the child and ends Run.
	Check func(ctx context.Context) error
	// Cleanup runs once when Run returns, before the supervisor pid file is
	// removed, unless that file could not be created.
	Cleanup func()
	// OnEvent receives the events of the supervision.
	OnEvent func(event Event)
	// Start starts every child; nil means StartCmd.
	Start StartFunc
}

// Op is the step of the supervision an Error comes from.
type Op int

const (
	// OpPidFile is creating or removing the supervisor pid file.
	OpPidFile Op = iota + 1
	// OpNext is getting a valid Spec from the Source.
	OpNext
	// OpStart is starting the child.
	OpStart
	// OpChildPidFile is writing or removing the child pid file.
	OpChildPidFile
	// OpRestart is asking the Source about a restart.
	OpRestart
	// OpCheck is the periodic check.
	OpCheck
)

// String describes the step.
func (op Op) String() string {
	switch op {
	case OpPidFile:
		return "the supervisor pid file"
	case OpNext:
		return "preparing the child"
	case OpStart:
		return "starting the child"
	case OpChildPidFile:
		return "the child pid file"
	case OpRestart:
		return "deciding on a restart"
	case OpCheck:
		return "the periodic check"
	}

	return "unknown step"
}

// Error is an error that ended Engine.Run, with the step it came from.
type Error struct {
	Op  Op
	Err error
}

// Error describes the failure.
func (err *Error) Error() string {
	return fmt.Sprintf("%s: %s", err.Op, err.Err)
}

// Unwrap returns the underlying error.
func (err *Error) Unwrap() error {
	return err.Err
}

// Engine supervises one child at a time. See the package documentation for
// the model.
type Engine struct {
	source Source
	opts   Options
	start  StartFunc
	// subscribe starts delivering signals; tests replace it to inject them.
	subscribe func() (<-chan os.Signal, func())
	ran       atomic.Bool
}

// New checks the options and returns an engine that is ready to Run.
func New(source Source, opts Options) (*Engine, error) {
	err := validateOptions(source, &opts)
	if err != nil {
		return nil, err
	}

	start := opts.Start
	if start == nil {
		start = StartCmd
	}

	return &Engine{
		source:    source,
		opts:      opts,
		start:     start,
		subscribe: subscribeOS,
		ran:       atomic.Bool{},
	}, nil
}

// uncatchable reports a signal the engine never receives.
func uncatchable(sig syscall.Signal) bool {
	switch sig {
	case syscall.SIGKILL, syscall.SIGSTOP, syscall.SIGURG, syscall.SIGCHLD:
		return true
	default:
		return false
	}
}

func validateOptions(source Source, opts *Options) error {
	if source == nil {
		return fmt.Errorf("%w: no source", ErrInvalidOptions)
	}

	for _, sig := range opts.StopSignals {
		if sig == 0 || uncatchable(sig) {
			return fmt.Errorf("%w: %s cannot be a stop signal", ErrInvalidOptions, sig)
		}
	}

	switch {
	case (opts.ReloadSignal == 0) != (opts.OnReload == nil):
		return fmt.Errorf("%w: the reload signal and hook go together", ErrInvalidOptions)
	case uncatchable(opts.ReloadSignal):
		return fmt.Errorf("%w: %s cannot be the reload signal", ErrInvalidOptions,
			opts.ReloadSignal)
	case slices.Contains(opts.StopSignals, opts.ReloadSignal):
		return fmt.Errorf("%w: %s is both a stop and the reload signal", ErrInvalidOptions,
			opts.ReloadSignal)
	case opts.RestartDelay < 0:
		return fmt.Errorf("%w: negative restart delay", ErrInvalidOptions)
	case opts.CheckPeriod < 0:
		return fmt.Errorf("%w: negative check period", ErrInvalidOptions)
	case (opts.CheckPeriod == 0) != (opts.Check == nil):
		return fmt.Errorf("%w: the check period and the check go together", ErrInvalidOptions)
	}

	return nil
}

// Run supervises until a stop, a cancelled ctx, an exit the Source does not
// restart, or a failure. It returns nil in the first three cases and an
// error that unwraps to *Error otherwise. Run can be called once.
func (e *Engine) Run(ctx context.Context) error {
	if !e.ran.CompareAndSwap(false, true) {
		return ErrAlreadyRun
	}

	signals, unsubscribe := e.subscribe()
	defer unsubscribe()

	pid := os.Getpid()

	if e.opts.PidFile != "" {
		err := createPidFile(e.opts.PidFile, pid)
		if err != nil {
			return &Error{Op: OpPidFile, Err: err}
		}

		e.emit(PidFileWritten{Path: e.opts.PidFile, Pid: pid})
	}

	err := e.loop(ctx, signals)

	if e.opts.Cleanup != nil {
		e.opts.Cleanup()
	}

	if e.opts.PidFile != "" {
		rmErr := removePidFile(e.opts.PidFile, pid)
		if rmErr != nil {
			err = errors.Join(err, &Error{Op: OpPidFile, Err: rmErr})
		}
	}

	return err
}

// loop starts the child, supervises it and restarts it until the end.
func (e *Engine) loop(ctx context.Context, signals <-chan os.Signal) error {
	for ctx.Err() == nil {
		spec, err := e.source.Next(ctx)
		if err != nil {
			return &Error{Op: OpNext, Err: err}
		}

		err = spec.validate()
		if err != nil {
			return &Error{Op: OpNext, Err: err}
		}

		proc, err := e.launch(ctx, &spec)
		if err != nil {
			return err
		}

		result := e.supervise(ctx, signals, proc, &spec)

		err = e.removeChildPidFile(proc.pid)
		e.emit(result.exit)

		switch {
		case result.checkErr != nil:
			return errors.Join(&Error{Op: OpCheck, Err: result.checkErr}, err)
		case err != nil:
			return err
		case result.stopped:
			return nil
		}

		restart, err := e.source.Restart(ctx, result.exit)
		if err != nil {
			return &Error{Op: OpRestart, Err: err}
		}

		if !restart || e.pause(ctx, signals) {
			return nil
		}
	}

	return nil
}

// launch starts the child and writes its pid file.
func (e *Engine) launch(ctx context.Context, spec *Spec) (*child, error) {
	proc, err := startChild(ctx, spec, e.start)
	if err != nil {
		return nil, &Error{Op: OpStart, Err: err}
	}

	if e.opts.ChildPidFile != "" {
		err = createPidFile(e.opts.ChildPidFile, proc.pid)
		if err != nil {
			e.emit(Killed{
				Pid:    proc.pid,
				Reason: KillChildPidFile,
				Err:    proc.signal(syscall.SIGKILL),
			})
			e.emit(<-proc.done)

			return nil, &Error{Op: OpChildPidFile, Err: err}
		}

		e.emit(PidFileWritten{Path: e.opts.ChildPidFile, Pid: proc.pid})
	}

	e.emit(Started{Pid: proc.pid})

	return proc, nil
}

func (e *Engine) removeChildPidFile(pid int) error {
	if e.opts.ChildPidFile == "" {
		return nil
	}

	err := removePidFile(e.opts.ChildPidFile, pid)
	if err != nil {
		return &Error{Op: OpChildPidFile, Err: err}
	}

	return nil
}

// outcome is how the supervision of one child ended.
type outcome struct {
	exit Exit
	// stopped tells that the exit followed a stop.
	stopped bool
	// checkErr is the failed check that killed the child.
	checkErr error
}

// supervision is the state of one running child.
type supervision struct {
	engine *Engine
	proc   *child
	spec   *Spec
	result outcome
	// killTimer fires Spec.StopTimeout after the first stop.
	killTimer *time.Timer
}

// supervise handles signals, checks and the stop of proc until it exits.
func (e *Engine) supervise(ctx context.Context, signals <-chan os.Signal, proc *child,
	spec *Spec,
) outcome {
	checks, stopChecks := e.startChecks(ctx)
	defer stopChecks()

	run := &supervision{
		engine:    e,
		proc:      proc,
		spec:      spec,
		result:    outcome{exit: Exit{Pid: 0, State: nil, Err: nil}, stopped: false, checkErr: nil},
		killTimer: nil,
	}
	defer run.stopKillTimer()

	cancelled := ctx.Done()

	for {
		select {
		case exit := <-proc.done:
			run.result.exit = exit

			return run.result
		case sig := <-signals:
			run.onSignal(sig)
		case <-cancelled:
			cancelled = nil

			_ = run.stop(spec.StopSignal)
		case <-run.killTimerC():
			run.killTimer = nil

			e.emit(Killed{
				Pid:    proc.pid,
				Reason: KillStopTimeout,
				Err:    proc.signal(syscall.SIGKILL),
			})
		case err := <-checks:
			run.onCheck(err)
		}
	}
}

// killTimerC is the channel of the kill timer, nil while none is armed.
func (run *supervision) killTimerC() <-chan time.Time {
	if run.killTimer == nil {
		return nil
	}

	return run.killTimer.C
}

func (run *supervision) stopKillTimer() {
	if run.killTimer != nil {
		run.killTimer.Stop()
	}
}

// stop sends sig to the child and arms the kill timer on the first stop.
func (run *supervision) stop(sig syscall.Signal) error {
	err := run.proc.signal(sig)

	if !run.result.stopped {
		run.result.stopped = true
		run.killTimer = time.NewTimer(run.spec.StopTimeout)
	}

	return err
}

// classify tells what the policy does with a received signal while a child
// is running. A signal that is not a syscall.Signal is dropped.
func (e *Engine) classify(received os.Signal) (syscall.Signal, SignalAction) {
	sig, ok := received.(syscall.Signal)

	switch {
	case !ok:
		return 0, ActionDrop
	case slices.Contains(e.opts.StopSignals, sig):
		return sig, ActionStop
	case e.opts.ReloadSignal != 0 && sig == e.opts.ReloadSignal:
		return sig, ActionReload
	default:
		return sig, ActionForward
	}
}

func (run *supervision) onSignal(received os.Signal) {
	engine := run.engine
	sig, action := engine.classify(received)

	var err error

	switch action {
	case ActionDrop:
		err = errUnknownSignal
	case ActionStop:
		out := run.spec.StopSignal
		if run.spec.ForwardStopSignal {
			out = sig
		}

		err = run.stop(out)
	case ActionReload:
		err = errors.Join(engine.opts.OnReload(), run.proc.signal(sig))
	case ActionForward:
		err = run.proc.signal(sig)
	}

	engine.emit(SignalReceived{Signal: sig, Action: action, Err: err})
}

func (run *supervision) onCheck(err error) {
	engine := run.engine
	engine.emit(Checked{Err: err})

	if err == nil || run.result.checkErr != nil {
		return
	}

	run.result.checkErr = err
	// The check failure decides the end now; a pending stop timeout would
	// only send a second SIGKILL.
	run.stopKillTimer()

	run.killTimer = nil

	engine.emit(Killed{
		Pid:    run.proc.pid,
		Reason: KillCheckFailed,
		Err:    run.proc.signal(syscall.SIGKILL),
	})
}

// startChecks runs the periodic check until the returned function is called,
// which waits for a running check to return.
func (e *Engine) startChecks(ctx context.Context) (<-chan error, func()) {
	if e.opts.CheckPeriod == 0 {
		return nil, func() {}
	}

	// The checks follow the child, not ctx: a check failing while a
	// cancelled context stops the child still kills it.
	checkCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	results := make(chan error)

	var checker sync.WaitGroup

	checker.Go(func() {
		ticker := time.NewTicker(e.opts.CheckPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-checkCtx.Done():
				return
			case <-ticker.C:
			}

			err := e.opts.Check(checkCtx)

			select {
			case results <- err:
			case <-checkCtx.Done():
				// The child is gone and nobody reads the result.
				return
			}

			if err != nil {
				return
			}
		}
	})

	return results, func() {
		cancel()
		checker.Wait()
	}
}

// pause waits the restart delay and reports whether a stop ended it.
func (e *Engine) pause(ctx context.Context, signals <-chan os.Signal) bool {
	e.emit(Restarting{Delay: e.opts.RestartDelay})

	timer := time.NewTimer(e.opts.RestartDelay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return false
		case <-ctx.Done():
			return true
		case received := <-signals:
			sig, action := e.classify(received)

			switch action {
			case ActionDrop:
				e.emit(SignalReceived{Signal: sig, Action: ActionDrop, Err: errUnknownSignal})
			case ActionStop:
				e.emit(SignalReceived{Signal: sig, Action: ActionStop, Err: nil})

				return true
			case ActionReload:
				e.emit(SignalReceived{Signal: sig, Action: ActionReload, Err: e.opts.OnReload()})
			case ActionForward:
				// No child to forward to.
				e.emit(SignalReceived{Signal: sig, Action: ActionDrop, Err: nil})
			}
		}
	}
}

func (e *Engine) emit(event Event) {
	if e.opts.OnEvent != nil {
		e.opts.OnEvent(event)
	}
}
