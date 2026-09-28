package supervisor

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"
)

// runState is what a Run holds at a given moment, so that its teardown can
// release all of it, even when a callback panics halfway.
type runState struct {
	// pidFile is the supervisor's own pid file.
	pidFile *pidFile
	// proc is the child from its start until it has been waited for.
	proc *child
	// childPidFile is the pid file of the current child.
	childPidFile *pidFile
	// stopChecks stops the periodic checks while they run.
	stopChecks func() error
}

// Run supervises until a stop, a cancelled ctx, an exit the Source does not
// restart, or a failure. It returns nil in the first three cases and an
// error that unwraps to *Error otherwise. Run can be called once.
//
// A panic in a callback tears the supervision down as described in the
// package documentation and then goes on up to the caller of Run.
func (e *Engine) Run(ctx context.Context) error {
	if !e.ran.CompareAndSwap(false, true) {
		return ErrAlreadyRun
	}

	signals, unsubscribe := e.subscribe()
	defer unsubscribe()

	state := &runState{pidFile: nil, proc: nil, childPidFile: nil, stopChecks: nil}
	pid := os.Getpid()

	if e.opts.PidFile != "" {
		owned, err := acquirePidFile(e.opts.PidFile, pid)
		if err != nil {
			return &Error{Op: OpPidFile, Err: err}
		}

		state.pidFile = owned
	}

	completed := false

	defer func() {
		if !completed {
			// A callback panicked; the panic goes on once this returns.
			_ = e.teardown(state)
		}
	}()

	if state.pidFile != nil {
		e.emit(PidFileWritten{Path: e.opts.PidFile, Pid: pid})
	}

	err := e.loop(ctx, signals, state)

	completed = true

	return errors.Join(err, e.teardown(state))
}

// teardown kills and waits for a child that still runs, stops the checks,
// removes the child pid file, runs Cleanup and removes the supervisor pid
// file. After a normal end of the loop only the last two have anything to do.
func (e *Engine) teardown(state *runState) error {
	if state.proc != nil {
		_ = state.proc.signal(syscall.SIGKILL)

		<-state.proc.done

		state.proc = nil
	}

	if state.stopChecks != nil {
		stop := state.stopChecks

		state.stopChecks = nil

		_ = stop()
	}

	childErr := e.releaseChildPidFile(state)

	return errors.Join(childErr, e.cleanUp(state))
}

// cleanUp runs Cleanup and then removes the supervisor pid file, even when
// Cleanup panics.
func (e *Engine) cleanUp(state *runState) error {
	var err error

	func() {
		defer func() {
			if state.pidFile == nil {
				return
			}

			rmErr := state.pidFile.release()

			state.pidFile = nil

			if rmErr != nil {
				err = &Error{Op: OpPidFile, Err: rmErr}
			}
		}()

		if e.opts.Cleanup != nil {
			e.opts.Cleanup()
		}
	}()

	return err
}

// loop starts the child, supervises it and restarts it until the end.
func (e *Engine) loop(ctx context.Context, signals <-chan os.Signal, state *runState) error {
	for {
		// A stop that is already waiting, before the first start or when
		// the restart delay runs out together with it, wins.
		if e.stopPending(ctx, signals) {
			return nil
		}

		spec, err := e.source.Next(ctx)
		if err != nil {
			return &Error{Op: OpNext, Err: err}
		}

		err = spec.validate()
		if err != nil {
			return &Error{Op: OpNext, Err: err}
		}

		// A stop that came while the Source prepared the Spec wins over
		// the start.
		if e.stopPending(ctx, signals) {
			return nil
		}

		err = e.launch(ctx, &spec, state)
		if err != nil {
			return err
		}

		result := e.supervise(ctx, signals, &spec, state)

		err = e.releaseChildPidFile(state)
		e.emit(result.exit)

		switch {
		case result.checkErr != nil:
			return errors.Join(&Error{Op: OpCheck, Err: result.checkErr}, err)
		case err != nil:
			return err
		case result.stopped:
			return nil
		}

		// A stop that is already waiting wins over a restart.
		if e.stopPending(ctx, signals) {
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
}

// launch starts the child and writes its pid file.
func (e *Engine) launch(ctx context.Context, spec *Spec, state *runState) error {
	proc, err := startChild(ctx, spec, e.start)
	if err != nil {
		return &Error{Op: OpStart, Err: err}
	}

	state.proc = proc

	if e.opts.ChildPidFile != "" {
		owned, err := acquirePidFile(e.opts.ChildPidFile, proc.pid)
		if err != nil {
			killErr := proc.signal(syscall.SIGKILL)
			exit := <-proc.done

			state.proc = nil

			e.emit(Killed{Pid: proc.pid, Reason: KillChildPidFile, Err: killErr})
			e.emit(exit)

			return &Error{Op: OpChildPidFile, Err: err}
		}

		state.childPidFile = owned

		e.emit(PidFileWritten{Path: e.opts.ChildPidFile, Pid: proc.pid})
	}

	e.emit(Started{Pid: proc.pid})

	return nil
}

// releaseChildPidFile removes the pid file of the child, if one was written.
func (e *Engine) releaseChildPidFile(state *runState) error {
	if state.childPidFile == nil {
		return nil
	}

	err := state.childPidFile.release()

	state.childPidFile = nil

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
	// checkErr is the failed check that ended the supervision.
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

// supervise handles signals, checks and the stop of the child until it
// exits.
func (e *Engine) supervise(ctx context.Context, signals <-chan os.Signal, spec *Spec,
	state *runState,
) outcome {
	proc := state.proc
	checks, stopChecks := e.startChecks(ctx)

	state.stopChecks = stopChecks

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
			state.proc = nil
			state.stopChecks = nil
			run.result.exit = exit

			// A check that was running as the child exited still counts.
			late := stopChecks()
			if late != nil {
				run.onLateCheck(late)
			}

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

// onLateCheck takes the failure of a check that finished after the child
// had exited: there is nothing left to kill, but it ends Run all the same.
func (run *supervision) onLateCheck(err error) {
	run.engine.emit(Checked{Err: err})

	if run.result.checkErr == nil {
		run.result.checkErr = err
	}
}

// startChecks runs the periodic check until the returned function is called.
// That function waits for a check still running and returns its failure,
// unless the check failed only because it was cancelled.
func (e *Engine) startChecks(ctx context.Context) (<-chan error, func() error) {
	if e.opts.CheckPeriod == 0 {
		return nil, func() error { return nil }
	}

	// The checks follow the child, not ctx: a check failing while a
	// cancelled context stops the child still kills it.
	checkCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	results := make(chan error)

	var (
		checker sync.WaitGroup
		late    error
	)

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
			if checkCtx.Err() != nil {
				// The child exited while the check ran. What the check found
				// still counts; its giving up on the cancellation does not.
				if !errors.Is(err, context.Canceled) {
					late = err
				}

				return
			}

			select {
			case results <- err:
			case <-checkCtx.Done():
				late = err

				return
			}

			if err != nil {
				return
			}
		}
	})

	return results, func() error {
		cancel()
		checker.Wait()

		return late
	}
}

// pause waits the restart delay and reports whether a stop ended it. A stop
// that is waiting as the delay runs out is taken by the loop before the next
// start.
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
			if e.onIdleSignal(received) {
				return true
			}
		}
	}
}

// stopPending handles, without waiting, the signals that have already
// arrived while no child runs, and reports whether one of them, or the
// context, asks to stop.
func (e *Engine) stopPending(ctx context.Context, signals <-chan os.Signal) bool {
	for {
		if ctx.Err() != nil {
			return true
		}

		select {
		case received := <-signals:
			if e.onIdleSignal(received) {
				return true
			}
		default:
			return false
		}
	}
}

// onIdleSignal handles a signal while no child runs and reports whether it
// is a stop: the reload hook runs, a signal to forward is dropped.
func (e *Engine) onIdleSignal(received os.Signal) bool {
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
		e.emit(SignalReceived{Signal: sig, Action: ActionDrop, Err: nil})
	}

	return false
}

func (e *Engine) emit(event Event) {
	if e.opts.OnEvent != nil {
		e.opts.OnEvent(event)
	}
}
