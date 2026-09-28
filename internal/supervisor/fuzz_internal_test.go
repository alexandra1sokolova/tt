package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The fuzz target drives the engine through a script decoded from the input:
// signals, exits, checks, time and the Source's answers, in the order the
// script gives them. The processes and the clock are fakes the script moves,
// so the only thing left to the scheduler is the order in which Go's select
// picks among channels that are ready together; each input is replayed a few
// times for that reason.
//
// Script layout: two header bytes, then two bytes per step (op, argument).
// Any byte string decodes, so the fuzzer never wastes an input. FUZZ_TRACE=1
// prints the events of a run to stderr, for reading a failing script.

const (
	// fuzzUnit is one unit of fake time.
	fuzzUnit        = time.Millisecond
	fuzzStopTimeout = 5 * fuzzUnit
	fuzzCheckPeriod = 3 * fuzzUnit
	// fuzzMaxSteps bounds the steps taken from one input.
	fuzzMaxSteps = 64
	// fuzzReplays is how many times one input runs.
	fuzzReplays = 3
	// fuzzSettleTimeout bounds the wait for the engine to settle; it is a
	// liveness check, not a pace.
	fuzzSettleTimeout = 10 * time.Second
	// fuzzFirstPid is the first pid of the fake children, far from real ones.
	fuzzFirstPid = 1 << 26
	// fuzzStalePid is a pid no process has.
	fuzzStalePid = 1<<31 - 2
)

// fuzzDelays are the restart delays a script picks from.
var fuzzDelays = [...]time.Duration{0, fuzzUnit, 4 * fuzzUnit, 20 * fuzzUnit}

// fuzzStops are the stop signals; a script picks one by its argument.
var fuzzStops = []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

const (
	fuzzReload  = syscall.SIGHUP
	fuzzForward = syscall.SIGUSR1
	fuzzIgnored = syscall.SIGUSR2
)

var (
	errFuzzCheck   = errors.New("fuzz: the check failed")
	errFuzzNext    = errors.New("fuzz: the Source has no Spec")
	errFuzzStart   = errors.New("fuzz: the child did not start")
	errFuzzRestart = errors.New("fuzz: the Source could not decide")
	errFuzzPanic   = errors.New("fuzz: a callback panicked")
)

// fuzzConfig is what the header bytes choose. The lowest bit of the first
// byte is spare.
type fuzzConfig struct {
	checks      bool
	forwardStop bool
	// stubborn children ignore the stop signals and die only of SIGKILL.
	stubborn bool
	// lateFails makes a check that is cancelled fail anyway, like a check
	// that found something and does not look at its context.
	lateFails    bool
	delay        time.Duration
	childPidFile bool
	// rival races the engine for a stale supervisor pid file.
	rival bool
}

type fuzzOp uint8

const (
	opStop        fuzzOp = iota // A stop signal; the argument picks which.
	opReload                    // The reload signal.
	opForward                   // A signal to forward.
	opIgnored                   // An ignored signal.
	opExit                      // The running child exits.
	opAdvance                   // Time advances by 1-16 units.
	opCheckPass                 // The check in progress, or the next one, passes.
	opCheckFail                 // The check in progress, or the next one, fails.
	opCancel                    // The context is cancelled.
	opFailNext                  // The next Source.Next fails.
	opFailStart                 // The next start fails.
	opRestartPlan               // The next Source.Restart says yes, no or fails.
	opInline                    // The next step happens inside a callback.
	opBatch                     // No settling after the next step.
	opPanic                     // A callback panics.
	opCount
)

// fuzzHook is a callback a step can be run inside, or can panic in.
type fuzzHook uint8

const (
	hookStarted fuzzHook = iota
	hookExit
	hookRestarting
	hookSignal
	hookChecked
	hookKilled
	hookNext
	hookRestart
	hookReload
	hookCount
)

type fuzzStep struct {
	op  fuzzOp
	arg uint8
}

func (step fuzzStep) String() string {
	names := [...]string{
		"stop", "reload", "forward", "ignored", "exit", "advance", "check-pass",
		"check-fail", "cancel", "fail-next", "fail-start", "restart-plan", "inline",
		"batch", "panic",
	}

	return fmt.Sprintf("%s(%d)", names[step.op], step.arg)
}

// decodeScript turns any byte string into a configuration and steps.
func decodeScript(data []byte) (fuzzConfig, []fuzzStep) {
	var header [2]byte

	copy(header[:], data)

	flags := header[0]
	cfg := fuzzConfig{
		checks:       flags&2 != 0,
		forwardStop:  flags&4 != 0,
		stubborn:     flags&8 != 0,
		lateFails:    flags&16 != 0,
		delay:        fuzzDelays[(flags>>5)&3],
		childPidFile: flags&128 != 0,
		rival:        header[1]&1 != 0,
	}

	var steps []fuzzStep

	if len(data) > len(header) {
		body := data[len(header):]
		for index := 0; index < len(body) && len(steps) < fuzzMaxSteps; index += 2 {
			step := fuzzStep{op: fuzzOp(body[index] % byte(opCount)), arg: 0}
			if index+1 < len(body) {
				step.arg = body[index+1]
			}

			steps = append(steps, step)
		}
	}

	return cfg, steps
}

// encodeScript is the inverse of decodeScript, for the seeds.
func encodeScript(flags, flags2 byte, steps ...fuzzStep) []byte {
	data := append(make([]byte, 0, 2+2*len(steps)), flags, flags2)

	for _, step := range steps {
		data = append(data, byte(step.op), step.arg)
	}

	return data
}

// fakeProc is a child the script drives.
type fakeProc struct {
	pid    int
	done   chan Exit
	exited bool
}

// reaped tells that the engine has taken the exit of the child.
func (proc *fakeProc) reaped() bool {
	return proc.exited && len(proc.done) == 0
}

// fakeTimer is a timer or ticker of the fake clock.
type fakeTimer struct {
	at, period time.Duration
	ch         chan time.Time
	stopped    bool
}

// fakeClock is a clock that moves only when the script advances it.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Duration
	timers []*fakeTimer
}

func (clk *fakeClock) after(d time.Duration) (<-chan time.Time, func()) {
	return clk.add(d, 0)
}

func (clk *fakeClock) every(d time.Duration) (<-chan time.Time, func()) {
	return clk.add(d, d)
}

func (clk *fakeClock) add(delay, period time.Duration) (<-chan time.Time, func()) {
	clk.mu.Lock()
	defer clk.mu.Unlock()

	timer := &fakeTimer{at: clk.now + delay, period: period, ch: make(chan time.Time, 1)}

	if delay <= 0 && period == 0 {
		// An expired timer is ready at once, as a real one is.
		timer.ch <- time.Time{}

		timer.stopped = true
	} else {
		clk.timers = append(clk.timers, timer)
	}

	return timer.ch, func() {
		clk.mu.Lock()
		defer clk.mu.Unlock()

		timer.stopped = true
	}
}

// advance moves the time and fires what is due, in order. A tick that finds
// its channel full is dropped, as with a real ticker.
func (clk *fakeClock) advance(d time.Duration) {
	clk.mu.Lock()
	defer clk.mu.Unlock()

	target := clk.now + d

	for {
		var next *fakeTimer

		for _, timer := range clk.timers {
			if !timer.stopped && timer.at <= target && (next == nil || timer.at < next.at) {
				next = timer
			}
		}

		if next == nil {
			break
		}

		clk.now = next.at

		select {
		case next.ch <- time.Time{}:
		default:
		}

		if next.period > 0 {
			next.at += next.period
		} else {
			next.stopped = true
		}
	}

	clk.now = target
	clk.timers = slices.DeleteFunc(clk.timers, func(timer *fakeTimer) bool {
		return timer.stopped
	})
}

// fuzzRun is one replay of a script.
type fuzzRun struct {
	t        *testing.T
	script   []byte
	cfg      fuzzConfig
	steps    []fuzzStep
	pidFile  string
	childPid string
	clock    *fakeClock
	signals  chan os.Signal
	ctx      context.Context //nolint:containedctx // The run's context is the script's to cancel.
	cancel   context.CancelFunc
	finished chan struct{}
	// completions carries the outcome the script gave the check.
	completions chan error

	// The fields below are guarded by mu.
	mu            sync.Mutex
	procs         []*fakeProc
	sent          []syscall.Signal
	handled       int
	stopRequested bool
	running       int
	failed        map[int]bool
	reported      map[int]bool
	checkFailed   bool
	failNext      int
	failStart     int
	restartPlan   []uint8
	used          map[error]bool
	restartSaidNo bool
	inline        [hookCount][]fuzzStep
	panicAt       [hookCount]bool
	cleanups      int
	ownedByEngine bool
	rivalOwned    bool
	result        error
	panicValue    any

	violationsMu sync.Mutex
	violations   []string
}

// locked runs change under mu.
func (run *fuzzRun) locked(change func()) {
	run.mu.Lock()
	defer run.mu.Unlock()

	change()
}

func (run *fuzzRun) failf(format string, args ...any) {
	run.violationsMu.Lock()
	defer run.violationsMu.Unlock()

	run.violations = append(run.violations, fmt.Sprintf(format, args...))
}

// stopFor tells whether sig is a stop signal.
func stopFor(sig syscall.Signal) bool {
	return slices.Contains(fuzzStops, sig)
}

// inject delivers a signal as the relay would. Under mu, so that the order
// of sent is the order of the channel.
func (run *fuzzRun) inject(sig syscall.Signal) {
	run.mu.Lock()
	defer run.mu.Unlock()

	run.sent = append(run.sent, sig)
	if stopFor(sig) {
		run.stopRequested = true
	}

	run.signals <- sig
}

// spawn is the engine's spawn: a fake child, and the checks that no child
// starts when it must not.
func (run *fuzzRun) spawn(context.Context, *Spec) (*child, error) {
	run.mu.Lock()
	defer run.mu.Unlock()

	if run.failStart > 0 {
		run.failStart--

		run.used[errFuzzStart] = true

		return nil, errFuzzStart
	}

	if run.stopRequested {
		run.failf("a child started after a stop was received")
	}

	if run.checkFailed {
		run.failf("a child started after a failed check")
	}

	for _, proc := range run.procs {
		if !proc.reaped() {
			run.failf("a child started while %d was not yet reaped", proc.pid)
		}
	}

	proc := &fakeProc{pid: fuzzFirstPid + len(run.procs), done: make(chan Exit, 1), exited: false}

	run.procs = append(run.procs, proc)

	return &child{
		pid:  proc.pid,
		done: proc.done,
		send: func(sig syscall.Signal) error { return run.deliver(proc, sig) },
	}, nil
}

// deliver is a signal reaching a fake child.
func (run *fuzzRun) deliver(proc *fakeProc, sig syscall.Signal) error {
	run.mu.Lock()
	defer run.mu.Unlock()

	if proc.exited {
		return fmt.Errorf("signalling %d: %w", proc.pid, os.ErrProcessDone)
	}

	if sig == syscall.SIGKILL || (stopFor(sig) && !run.cfg.stubborn) {
		run.exitLocked(proc)
	}

	return nil
}

func (run *fuzzRun) exitLocked(proc *fakeProc) {
	proc.exited = true
	proc.done <- Exit{Pid: proc.pid, State: nil, Err: nil}
}

// check is Options.Check: it waits for the script's outcome, or for the
// cancellation that comes when the child exits.
func (run *fuzzRun) check(ctx context.Context) error {
	var owner int

	run.locked(func() { owner = run.running })

	if owner == 0 {
		run.failf("a check ran while no child was running")
	}

	var err error

	select {
	case err = <-run.completions:
	case <-ctx.Done():
		err = fmt.Errorf("check interrupted: %w", ctx.Err())
		if run.cfg.lateFails {
			err = errFuzzCheck
		}
	}

	if errors.Is(err, errFuzzCheck) {
		run.locked(func() {
			run.failed[owner] = true
			run.checkFailed = true
		})
	}

	return err
}

// hook runs the steps the script put inside a callback, then panics there if
// the script says so.
func (run *fuzzRun) hook(kind fuzzHook) {
	var (
		steps  []fuzzStep
		panics bool
	)

	run.locked(func() {
		steps, run.inline[kind] = run.inline[kind], nil
		panics, run.panicAt[kind] = run.panicAt[kind], false
	})

	for _, step := range steps {
		run.apply(step)
	}

	if panics {
		panic(errFuzzPanic)
	}
}

func (run *fuzzRun) pidFileHolds(path string) (int, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		run.failf("%s holds %q", path, data)
	}

	return pid, true
}

// wantAction is what the engine has to do with sig in the current state.
func (run *fuzzRun) wantAction(sig syscall.Signal) SignalAction {
	switch {
	case stopFor(sig):
		return ActionStop
	case sig == fuzzReload:
		return ActionReload
	case sig == fuzzIgnored:
		return ActionIgnore
	case run.running != 0:
		return ActionForward
	default:
		return ActionDrop
	}
}

// onEvent is Options.OnEvent: it follows the events and checks each against
// the model.
func (run *fuzzRun) onEvent(event Event) {
	if os.Getenv("FUZZ_TRACE") != "" {
		fmt.Fprintf(os.Stderr, "TRACE %T %+v\n", event, event)
	}

	run.mu.Lock()

	var kind fuzzHook

	switch event := event.(type) {
	case PidFileWritten:
		kind = hookCount

		if event.Path == run.pidFile {
			run.ownedByEngine = true
		}
	case Started:
		kind = hookStarted

		run.onStarted(event)
	case Exit:
		kind = hookExit

		run.onExit(event)
	case SignalReceived:
		kind = hookSignal

		run.onSignal(event)
	case Checked:
		kind = hookChecked

		switch {
		case event.Err == nil:
		case !errors.Is(event.Err, errFuzzCheck):
			run.failf("a check reported %v", event.Err)
		case run.running == 0:
			run.failf("a failed check reported with no child")
		default:
			run.reported[run.running] = true
		}
	case Killed:
		kind = hookKilled
	case Restarting:
		kind = hookRestarting
	}

	run.mu.Unlock()

	if kind != hookCount {
		run.hook(kind)
	}
}

func (run *fuzzRun) onStarted(event Started) {
	if run.running != 0 {
		run.failf("%d started while %d runs", event.Pid, run.running)
	}

	run.running = event.Pid

	if run.cfg.childPidFile {
		pid, ok := run.pidFileHolds(run.childPid)
		if !ok || pid != event.Pid {
			run.failf("at the start of %d the child pid file holds %d (present %v)",
				event.Pid, pid, ok)
		}
	}
}

func (run *fuzzRun) onExit(event Exit) {
	if event.Pid != run.running {
		run.failf("%d exited while %d was the child", event.Pid, run.running)
	}

	run.running = 0

	if _, ok := run.pidFileHolds(run.childPid); ok {
		run.failf("the child pid file outlived %d", event.Pid)
	}

	// Every failure of a check of this child has been reported by now.
	if run.failed[event.Pid] != run.reported[event.Pid] {
		run.failf("the check of %d failed: %v, reported: %v", event.Pid,
			run.failed[event.Pid], run.reported[event.Pid])
	}
}

func (run *fuzzRun) onSignal(event SignalReceived) {
	if run.handled >= len(run.sent) {
		run.failf("%s handled but never sent", event.Signal)

		return
	}

	sig := run.sent[run.handled]
	run.handled++

	if event.Signal != sig {
		run.failf("%s handled where %s was sent", event.Signal, sig)
	}

	want := run.wantAction(sig)
	if event.Action != want {
		run.failf("%s handled as %s, want %s (child %d)", sig, event.Action, want, run.running)
	}

	if event.Err != nil && !errors.Is(event.Err, os.ErrProcessDone) {
		run.failf("%s handled with %v", sig, event.Err)
	}
}

// next is Source.Next.
func (run *fuzzRun) next(context.Context) (Spec, error) {
	run.mu.Lock()

	if run.stopRequested {
		run.failf("the Source was asked for a Spec after a stop")
	}

	var err error

	if run.failNext > 0 {
		run.failNext--

		run.used[errFuzzNext] = true
		err = errFuzzNext
	}

	run.mu.Unlock()
	run.hook(hookNext)

	spec := Spec{
		Path:              "fuzz",
		Args:              nil,
		Env:               nil,
		Dir:               "",
		Stdin:             nil,
		Stdout:            nil,
		Stderr:            nil,
		StopSignal:        syscall.SIGTERM,
		ForwardStopSignal: run.cfg.forwardStop,
		StopTimeout:       fuzzStopTimeout,
		ProcessGroup:      false,
	}

	return spec, err
}

// restart is Source.Restart.
func (run *fuzzRun) restart(exit Exit) (bool, error) {
	run.mu.Lock()

	if run.stopRequested {
		run.failf("the Source was asked about a restart after a stop")
	}

	// The Source here says yes unless the script says otherwise, so a
	// failed check that reached it would be restarted over.
	if run.checkFailed {
		run.failf("the Source was asked about a restart of %d after a failed check", exit.Pid)
	}

	restart, err := true, error(nil)

	if len(run.restartPlan) > 0 {
		switch run.restartPlan[0] % 3 {
		case 1:
			restart = false
			run.restartSaidNo = true
		case 2:
			restart, err = false, errFuzzRestart
			run.used[errFuzzRestart] = true
		}

		run.restartPlan = run.restartPlan[1:]
	}

	run.mu.Unlock()
	run.hook(hookRestart)

	return restart, err
}

// fuzzSource adapts a fuzzRun to Source.
type fuzzSource struct{ run *fuzzRun }

func (src fuzzSource) Next(ctx context.Context) (Spec, error) { return src.run.next(ctx) }

func (src fuzzSource) Restart(_ context.Context, exit Exit) (bool, error) {
	return src.run.restart(exit)
}

func (run *fuzzRun) cleanup() {
	run.mu.Lock()
	defer run.mu.Unlock()

	run.cleanups++

	for _, proc := range run.procs {
		if !proc.reaped() {
			run.failf("Cleanup ran while %d was not reaped", proc.pid)
		}
	}

	run.checkOwnPidFile("Cleanup")
}

// checkOwnPidFile checks that the supervisor pid file, which the engine owns
// from the event that reports it written until Cleanup has run, names the
// engine all that time: nobody took it over.
func (run *fuzzRun) checkOwnPidFile(when string) {
	if !run.ownedByEngine || run.cleanups > 1 {
		return
	}

	pid, ok := run.pidFileHolds(run.pidFile)
	if !ok || pid != os.Getpid() {
		run.failf("at %s the supervisor pid file holds %d (present %v)", when, pid, ok)
	}
}

// apply performs one step: from the script, or inside a callback.
func (run *fuzzRun) apply(step fuzzStep) {
	switch step.op {
	case opStop:
		run.inject(fuzzStops[int(step.arg)%len(fuzzStops)])
	case opReload:
		run.inject(fuzzReload)
	case opForward:
		run.inject(fuzzForward)
	case opIgnored:
		run.inject(fuzzIgnored)
	case opExit:
		run.locked(func() {
			for _, proc := range run.procs {
				if !proc.exited {
					run.exitLocked(proc)
				}
			}
		})
	case opAdvance:
		run.clock.advance(time.Duration(step.arg%16+1) * fuzzUnit)
	case opCheckPass, opCheckFail:
		outcome := error(nil)
		if step.op == opCheckFail {
			outcome = errFuzzCheck
		}

		select {
		case run.completions <- outcome:
		default:
		}
	case opCancel:
		run.locked(func() { run.stopRequested = true })
		run.cancel()
	case opFailNext:
		run.locked(func() { run.failNext++ })
	case opFailStart:
		run.locked(func() { run.failStart++ })
	case opRestartPlan:
		run.locked(func() { run.restartPlan = append(run.restartPlan, step.arg) })
	case opInline, opBatch, opPanic, opCount:
		// Meaningless inside a callback.
	}
}

// engineStacks returns the stacks of the goroutines of the engine: the one
// running Run, recognised by the closure that calls it even before it has
// got that far, and the checker.
func engineStacks() []string {
	buf := make([]byte, 1<<16)

	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]

			break
		}

		buf = make([]byte, 2*len(buf))
	}

	var stacks []string

	for stack := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(stack, "supervisor.(*Engine)") ||
			strings.Contains(stack, "supervisor.(*fuzzRun).drive.func") {
			stacks = append(stacks, stack)
		}
	}

	return stacks
}

// quiet tells whether every goroutine of the engine waits in a select. As
// every channel it waits on is made ready by the script alone, the engine
// then cannot move until the script does.
func quiet() bool {
	for _, stack := range engineStacks() {
		header, _, _ := strings.Cut(stack, "\n")
		if !strings.Contains(header, "[select") {
			return false
		}
	}

	return true
}

func (run *fuzzRun) isFinished() bool {
	select {
	case <-run.finished:
		return true
	default:
		return false
	}
}

// settle waits until the engine can move no further on its own.
func (run *fuzzRun) settle() {
	deadline := time.Now().Add(fuzzSettleTimeout)

	for spin := 0; ; spin++ {
		if run.isFinished() || quiet() {
			return
		}

		if time.Now().After(deadline) {
			run.t.Fatalf("the engine did not settle\nscript %s\n%s", run.describe(),
				strings.Join(engineStacks(), "\n\n"))
		}

		if spin < 100 {
			runtime.Gosched()
		} else {
			time.Sleep(20 * time.Microsecond)
		}
	}
}

// checkSettled checks the invariants that hold whenever the engine rests.
func (run *fuzzRun) checkSettled() {
	run.mu.Lock()
	defer run.mu.Unlock()

	alive := 0

	for _, proc := range run.procs {
		if !proc.exited {
			alive++
		}
	}

	if alive > 1 {
		run.failf("%d children alive at once", alive)
	}

	if !run.isFinished() && run.cleanups == 0 {
		run.checkOwnPidFile("a settled step")
	}

	if run.cfg.childPidFile && !run.isFinished() {
		pid, ok := run.pidFileHolds(run.childPid)

		switch {
		case run.running != 0 && (!ok || pid != run.running):
			run.failf("child %d runs, its pid file holds %d (present %v)", run.running, pid, ok)
		case run.running == 0 && ok:
			run.failf("no child runs, its pid file holds %d", pid)
		}
	}
}

func (run *fuzzRun) describe() string {
	parts := make([]string, 0, len(run.steps))
	for _, step := range run.steps {
		parts = append(parts, step.String())
	}

	return fmt.Sprintf("%+v %s", run.cfg, strings.Join(parts, " "))
}

// drive runs the script against a fresh engine and checks the end state.
func (run *fuzzRun) drive() {
	engine, err := New(fuzzSource{run: run}, Options{
		PidFile:       run.pidFile,
		ChildPidFile:  childPidFileOf(run),
		StopSignals:   fuzzStops,
		IgnoreSignals: []syscall.Signal{fuzzIgnored},
		ReloadSignal:  fuzzReload,
		OnReload: func() error {
			run.hook(hookReload)

			return nil
		},
		RestartDelay: run.cfg.delay,
		CheckPeriod:  checkPeriodOf(run.cfg),
		Check:        checkOf(run),
		Cleanup:      run.cleanup,
		OnEvent:      run.onEvent,
		Start:        nil,
	})
	if err != nil {
		run.t.Fatal(err)
	}

	engine.subscribe = func() (<-chan os.Signal, func()) { return run.signals, func() {} }
	engine.spawn = run.spawn
	engine.clock = run.clock

	rival := run.startRival()

	go func() {
		defer close(run.finished)
		defer func() { run.panicValue = recover() }()

		run.result = engine.Run(run.ctx)
	}()

	run.settle()

	batch := false

	for index := 0; index < len(run.steps) && !run.isFinished(); index++ {
		step := run.steps[index]

		switch step.op {
		case opInline:
			if index+1 < len(run.steps) {
				kind, inner := fuzzHook(step.arg%byte(hookCount)), run.steps[index+1]

				run.locked(func() { run.inline[kind] = append(run.inline[kind], inner) })

				index++
			}

			continue
		case opPanic:
			run.locked(func() { run.panicAt[step.arg%byte(hookCount)] = true })

			continue
		case opBatch:
			batch = true

			continue
		default:
			run.apply(step)
		}

		if batch {
			batch = false

			continue
		}

		run.settle()
		run.checkSettled()
	}

	run.settle()
	run.finish()
	rival()
	run.checkEnd()
}

func childPidFileOf(run *fuzzRun) string {
	if run.cfg.childPidFile {
		return run.childPid
	}

	return ""
}

func checkPeriodOf(cfg fuzzConfig) time.Duration {
	if cfg.checks {
		return fuzzCheckPeriod
	}

	return 0
}

func checkOf(run *fuzzRun) func(ctx context.Context) error {
	if run.cfg.checks {
		return run.check
	}

	return nil
}

// startRival races the engine for a stale supervisor pid file, if the script
// asks for it, and returns the function that gives the file up.
func (run *fuzzRun) startRival() func() {
	if !run.cfg.rival {
		return func() {}
	}

	err := os.WriteFile(run.pidFile, []byte(strconv.Itoa(fuzzStalePid)), 0o600)
	if err != nil {
		run.t.Fatal(err)
	}

	result := make(chan *pidFile, 1)

	go func() {
		owned, _ := acquirePidFile(run.pidFile, os.Getppid())
		result <- owned
	}()

	return func() {
		owned := <-result
		if owned == nil {
			return
		}

		run.locked(func() { run.rivalOwned = true })

		_ = owned.release()
	}
}

// finish ends a run the script left going: a stop, time for the stop
// timeout, and a cancelled context if that is not enough.
func (run *fuzzRun) finish() {
	if run.isFinished() {
		return
	}

	run.inject(syscall.SIGTERM)
	run.settle()

	for range 3 {
		if run.isFinished() {
			return
		}

		run.clock.advance(fuzzStopTimeout + fuzzUnit)
		run.settle()
	}

	if !run.isFinished() {
		run.failf("Run went on after a stop and the stop timeout")
		run.cancel()
		run.clock.advance(fuzzStopTimeout + fuzzUnit)
		run.settle()
	}

	select {
	case <-run.finished:
	case <-time.After(fuzzSettleTimeout):
		run.t.Fatalf("Run does not return\nscript %s\n%s", run.describe(),
			strings.Join(engineStacks(), "\n\n"))
	}
}

// checkEnd checks the state Run leaves behind.
func (run *fuzzRun) checkEnd() {
	// The engine's goroutines end with Run.
	deadline := time.Now().Add(fuzzSettleTimeout)
	for len(engineStacks()) > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Microsecond)
	}

	if stacks := engineStacks(); len(stacks) > 0 {
		run.failf("goroutines outlived Run:\n%s", strings.Join(stacks, "\n\n"))
	}

	run.mu.Lock()
	defer run.mu.Unlock()

	for _, proc := range run.procs {
		if !proc.reaped() {
			run.failf("Run returned while %d was not reaped", proc.pid)
		}
	}

	if !run.ownedByEngine {
		// A rival owned the pid file first: nothing ran, nothing was
		// cleaned. A rival that owned it after Run is no conflict.
		if !run.rivalOwned {
			run.failf("the engine never owned its pid file, and no rival did")
		}

		run.checkResult(OpPidFile)

		return
	}

	if run.cleanups != 1 {
		run.failf("Cleanup ran %d times", run.cleanups)
	}

	for _, path := range []string{run.pidFile, run.childPid} {
		if _, ok := run.pidFileHolds(path); ok {
			run.failf("%s outlived Run", path)
		}
	}

	if run.panicValue != nil {
		if run.panicValue != errFuzzPanic { //nolint:errorlint // The very value panicked with.
			run.failf("Run panicked with %v", run.panicValue)
		}

		return
	}

	if run.running != 0 {
		run.failf("Run returned without reporting the exit of %d", run.running)
	}

	run.checkResult(0)
}

// checkResult checks the error Run returned against what the script caused.
func (run *fuzzRun) checkResult(forced Op) {
	var runErr *Error

	isError := errors.As(run.result, &runErr)

	switch {
	case forced != 0:
		if !isError || runErr.Op != forced {
			run.failf("Run returned %v, want %s", run.result, forced)
		}
	case run.checkFailed:
		if !isError || runErr.Op != OpCheck || !errors.Is(run.result, errFuzzCheck) {
			run.failf("Run returned %v after a failed check", run.result)
		}
	case run.result == nil:
		if !run.stopRequested && !run.restartSaidNo {
			run.failf("Run returned nil without a stop or a declined restart")
		}
	case !isError:
		run.failf("Run returned %v", run.result)
	default:
		causes := map[Op]error{
			OpNext:    errFuzzNext,
			OpStart:   errFuzzStart,
			OpRestart: errFuzzRestart,
		}

		cause, known := causes[runErr.Op]
		if !known || !run.used[cause] || !errors.Is(run.result, cause) {
			run.failf("Run returned %v, which the script did not cause", run.result)
		}
	}
}

// replayScript runs a script fuzzReplays times, failing t on a violation.
func replayScript(t *testing.T, data []byte) {
	t.Helper()

	cfg, steps := decodeScript(data)

	for replay := range fuzzReplays {
		dir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		run := &fuzzRun{
			t:           t,
			script:      data,
			cfg:         cfg,
			steps:       steps,
			pidFile:     filepath.Join(dir, "supervisor.pid"),
			childPid:    filepath.Join(dir, "child.pid"),
			clock:       &fakeClock{now: 0, timers: nil},
			signals:     make(chan os.Signal, 4*fuzzMaxSteps),
			ctx:         ctx,
			cancel:      cancel,
			finished:    make(chan struct{}),
			completions: make(chan error, 1),
			failed:      map[int]bool{},
			reported:    map[int]bool{},
			used:        map[error]bool{},
		}

		run.drive()
		cancel()

		if len(run.violations) > 0 {
			t.Fatalf("replay %d of script %x\n%s\nviolations:\n  %s", replay, data,
				run.describe(), strings.Join(run.violations, "\n  "))
		}
	}
}

// Flags of the first header byte, for the seeds.
const (
	flagChecks       byte = 2
	flagForwardStop  byte = 4
	flagStubborn     byte = 8
	flagLateFails    byte = 16
	flagDelay1       byte = 1 << 5
	flagDelay20      byte = 3 << 5
	flagChildPidFile byte = 128
	flag2Rival       byte = 1
)

func step(op fuzzOp, arg uint8) fuzzStep { return fuzzStep{op: op, arg: arg} }

func inlineIn(kind fuzzHook, inner fuzzStep) []fuzzStep {
	return []fuzzStep{step(opInline, uint8(kind)), inner}
}

// fuzzSeeds are the scenarios the review found, and a few ordinary ones.
func fuzzSeeds() map[string][]byte {
	seq := slices.Concat[[]fuzzStep]
	one := func(steps ...fuzzStep) []fuzzStep { return steps }

	return map[string][]byte{
		// A check fails as the child exits under it (review P1); the Source
		// would restart the child.
		"check-races-exit": encodeScript(flagChecks|flagLateFails|flagChildPidFile, 0,
			step(opAdvance, 2), step(opExit, 0)),
		// The same with a restart delay and a stop in it, as the review
		// reproduced it.
		"check-races-exit-then-stop": encodeScript(flagChecks|flagLateFails|flagDelay20, 0,
			step(opAdvance, 2), step(opExit, 0), step(opCancel, 0)),
		// A stop queued while the child exits, no restart delay (review
		// P2): the stop and the exit are ready together.
		"stop-queued-with-exit": encodeScript(flagChildPidFile, 0, seq(
			inlineIn(hookStarted, step(opStop, 1)), inlineIn(hookStarted, step(opExit, 0)),
			one(step(opExit, 0)))...),
		// A stop queued as the restart delay begins.
		"stop-queued-with-delay": encodeScript(0, 0, seq(
			inlineIn(hookRestarting, step(opStop, 0)), one(step(opExit, 0)))...),
		// A stop while the Source prepares the next Spec.
		"stop-during-next": encodeScript(0, 0, seq(
			inlineIn(hookNext, step(opStop, 1)), one(step(opExit, 0)))...),
		// A callback panics (review P2): at a start, on a signal, on a check.
		"panic-on-started": encodeScript(flagChildPidFile, 0,
			step(opPanic, uint8(hookStarted)), step(opExit, 0)),
		"panic-on-signal": encodeScript(flagChecks|flagChildPidFile, 0,
			step(opPanic, uint8(hookSignal)), step(opForward, 0)),
		"panic-on-checked": encodeScript(flagChecks|flagStubborn, 0,
			step(opPanic, uint8(hookChecked)), step(opAdvance, 2), step(opCheckPass, 0)),
		// Two starters race for a stale supervisor pid file (review P1).
		"rival-for-stale-pid-file": encodeScript(0, flag2Rival, step(opForward, 0)),
		// Ordinary lives: escalation of a stubborn child, a failed check
		// under each policy, Source failures, ignored and reloaded signals.
		"stubborn-escalation": encodeScript(flagStubborn|flagChildPidFile, 0,
			step(opStop, 2), step(opAdvance, 4), step(opAdvance, 4)),
		"check-fails-stop": encodeScript(flagChecks|flagStubborn, 0,
			step(opCheckFail, 0), step(opAdvance, 2)),
		"check-passes-then-fails": encodeScript(flagChecks|flagDelay1, 0,
			step(opCheckPass, 0), step(opAdvance, 2), step(opAdvance, 2),
			step(opCheckFail, 0), step(opAdvance, 2)),
		"source-failures": encodeScript(flagDelay1, 0,
			step(opRestartPlan, 0), step(opExit, 0), step(opAdvance, 1), step(opFailStart, 0),
			step(opExit, 0), step(opAdvance, 1)),
		"signals-idle-and-running": encodeScript(flagDelay20|flagForwardStop, 0,
			step(opIgnored, 0), step(opReload, 0), step(opForward, 0), step(opExit, 0),
			step(opForward, 0), step(opReload, 0), step(opIgnored, 0), step(opAdvance, 15),
			step(opAdvance, 5), step(opStop, 2)),
		"batched-cancel-and-exit": encodeScript(0, 0,
			step(opBatch, 0), step(opExit, 0), step(opCancel, 0)),
		"restart-declined": encodeScript(0, 0, step(opRestartPlan, 1), step(opExit, 0)),
	}
}

// FuzzEngine drives the engine through scripts and checks its invariants
// after every step.
//
// FUZZ_NO_SEEDS=1 starts from an empty script instead of the seeds, to
// measure how long the fuzzer takes to find a defect on its own.
func FuzzEngine(f *testing.F) {
	seeds := fuzzSeeds()
	if os.Getenv("FUZZ_NO_SEEDS") != "" {
		seeds = map[string][]byte{"empty": encodeScript(0, 0)}
	}

	for _, name := range slices.Sorted(maps.Keys(seeds)) {
		f.Add(seeds[name])
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		replayScript(t, data)
	})
}

// TestFuzzSeeds replays each seed of FuzzEngine by name, so that a failing
// one says which scenario broke.
func TestFuzzSeeds(t *testing.T) {
	seeds := fuzzSeeds()
	for _, name := range slices.Sorted(maps.Keys(seeds)) {
		t.Run(name, func(t *testing.T) { replayScript(t, seeds[name]) })
	}
}

// TestFuzzSeedsDecode pins that the seeds decode to the steps they were built
// from, so a seed tests the scenario its name says.
func TestFuzzSeedsDecode(t *testing.T) {
	data := encodeScript(flagChecks, flag2Rival, step(opInline, 3), step(opStop, 1))
	cfg, steps := decodeScript(data)

	if !cfg.checks || !cfg.rival || cfg.stubborn {
		t.Fatalf("header decoded as %+v", cfg)
	}

	if !slices.Equal(steps, []fuzzStep{step(opInline, 3), step(opStop, 1)}) {
		t.Fatalf("steps decoded as %v", steps)
	}

	_, empty := decodeScript(nil)
	if len(empty) != 0 || !bytes.Equal(encodeScript(0, 0), []byte{0, 0}) {
		t.Fatal("an empty script is not empty")
	}
}
