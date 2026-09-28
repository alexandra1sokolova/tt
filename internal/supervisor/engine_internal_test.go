package supervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stopSignals = []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// TestStopSignal pins which signal a stop sends to the child: the received
// one with ForwardStopSignal, Spec.StopSignal otherwise. A stopped child is
// not restarted even though the Source would restart it.
func TestStopSignal(t *testing.T) {
	cases := []struct {
		name     string
		forward  bool
		received syscall.Signal
		code     int
	}{
		{name: "forward SIGINT", forward: true, received: syscall.SIGINT, code: exitOnInt},
		{name: "forward SIGQUIT", forward: true, received: syscall.SIGQUIT, code: exitOnQuit},
		{name: "translate SIGINT", forward: false, received: syscall.SIGINT, code: exitOnTerm},
		{name: "translate SIGQUIT", forward: false, received: syscall.SIGQUIT, code: exitOnTerm},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			spec := helperSpec(t, dir, modeServe)

			spec.ForwardStopSignal = test.forward

			src := fixedSource(spec, true)
			sup := newHarness(t, src, Options{StopSignals: stopSignals})
			sup.start()

			started := waitEvent[Started](t, sup.rec, 1)
			waitReady(t, dir, started.Pid)
			sup.send(test.received)

			require.NoError(t, sup.wait())

			exits, _ := eventsOf[Exit](sup.rec)
			require.Len(t, exits, 1)
			assert.Equal(t, test.code, exits[0].State.ExitCode())

			kills, _ := eventsOf[Killed](sup.rec)
			assert.Empty(t, kills)

			nexts, restarts := src.calls()
			assert.Equal(t, 1, nexts)
			assert.Zero(t, restarts)
			sup.assertNoChildren()
		})
	}
}

// TestStopEscalatesToKill pins the SIGKILL sent to a child that outlives the
// stop timeout.
func TestStopEscalatesToKill(t *testing.T) {
	const stopTimeout = 300 * time.Millisecond

	dir := t.TempDir()
	spec := helperSpec(t, dir, modeStubborn)

	spec.StopTimeout = stopTimeout

	sup := newHarness(t, fixedSource(spec, true), Options{StopSignals: stopSignals})
	sup.start()

	started := waitEvent[Started](t, sup.rec, 1)
	waitReady(t, dir, started.Pid)

	stopAt := time.Now()

	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())

	kills, killTimes := eventsOf[Killed](sup.rec)
	require.Len(t, kills, 1)
	assert.Equal(t, KillStopTimeout, kills[0].Reason)
	require.NoError(t, kills[0].Err)
	assert.GreaterOrEqual(t, killTimes[0].Sub(stopAt), stopTimeout)

	exits, _ := eventsOf[Exit](sup.rec)
	require.Len(t, exits, 1)
	assert.Equal(t, syscall.SIGKILL, termSignal(exits[0].State))
	sup.assertNoChildren()
}

// TestRepeatedStopKeepsTimeout pins that a second stop signal reaches the
// child but does not push the SIGKILL back.
func TestRepeatedStopKeepsTimeout(t *testing.T) {
	const (
		stopTimeout = time.Second
		secondAfter = stopTimeout / 2
	)

	dir := t.TempDir()
	spec := helperSpec(t, dir, modeStubborn)

	spec.StopTimeout = stopTimeout

	var (
		sup  *harness
		sent atomic.Bool
	)

	rec := &recorder{}

	sup = newHarness(t, fixedSource(spec, true), Options{
		StopSignals: stopSignals,
		OnEvent: func(event Event) {
			rec.record(event)

			// The loop sends itself the second stop while it handles the
			// first, so the second is always handled well before the kill
			// timer can fire, however the scheduler treats the test.
			received, ok := event.(SignalReceived)
			if ok && received.Action == ActionStop && !sent.Swap(true) {
				time.Sleep(secondAfter)
				sup.send(syscall.SIGINT)
			}
		},
	})

	sup.rec = rec
	sup.start()

	started := waitEvent[Started](t, rec, 1)
	waitReady(t, dir, started.Pid)
	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())

	stops, stopTimes := eventsOf[SignalReceived](rec)
	require.Len(t, stops, 2)

	for _, stop := range stops {
		assert.Equal(t, ActionStop, stop.Action)
		require.NoError(t, stop.Err)
	}

	kills, killTimes := eventsOf[Killed](rec)
	require.Len(t, kills, 1)
	require.True(t, stopTimes[1].Before(killTimes[0]))

	// Kept, the kill comes a stop timeout after the first stop; re-armed, it
	// would come half a timeout later.
	sinceFirst := killTimes[0].Sub(stopTimes[0])
	assert.GreaterOrEqual(t, sinceFirst, stopTimeout-secondAfter/10)
	assert.Less(t, sinceFirst, stopTimeout+secondAfter/2)
}

// TestRestartAsDecided pins that the Source decides every restart, is given
// the exit, and supplies a Spec before every start.
func TestRestartAsDecided(t *testing.T) {
	dir := t.TempDir()

	var exits []Exit

	src := &testSource{
		next: func(_ context.Context, n int) (Spec, error) {
			spec := helperSpec(t, dir, modeExit, codeEnv+"=3")

			spec.Args = []string{"run-" + strconv.Itoa(n)}

			return spec, nil
		},
		restart: func(n int, exit Exit) (bool, error) {
			exits = append(exits, exit)

			return n < 3, nil
		},
	}
	sup := newHarness(t, src,
		Options{StopSignals: stopSignals, RestartDelay: 10 * time.Millisecond})
	sup.start()
	require.NoError(t, sup.wait())

	nexts, restarts := src.calls()
	assert.Equal(t, 3, nexts)
	assert.Equal(t, 3, restarts)

	started, _ := eventsOf[Started](sup.rec)
	require.Len(t, started, 3)
	require.Len(t, exits, 3)

	for index, exit := range exits {
		assert.Equal(t, started[index].Pid, exit.Pid)
		assert.Equal(t, 3, exit.State.ExitCode())

		var exitErr *exec.ExitError

		require.ErrorAs(t, exit.Err, &exitErr)

		args, err := os.ReadFile(helperFile(dir, "args", exit.Pid))
		require.NoError(t, err)
		assert.Equal(t, "run-"+strconv.Itoa(index+1), string(args),
			"the Spec of start %d", index+1)
	}

	delays, _ := eventsOf[Restarting](sup.rec)
	assert.Len(t, delays, 2)
	sup.assertNoChildren()
}

// TestRestartDelay pins the pause between an exit and the next start.
func TestRestartDelay(t *testing.T) {
	const delay = 400 * time.Millisecond

	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeExit, codeEnv+"=1"), true)

	src.restart = func(n int, _ Exit) (bool, error) { return n < 2, nil }

	sup := newHarness(t, src, Options{StopSignals: stopSignals, RestartDelay: delay})
	sup.start()
	require.NoError(t, sup.wait())

	_, exitTimes := eventsOf[Exit](sup.rec)
	started, startTimes := eventsOf[Started](sup.rec)
	require.Len(t, started, 2)
	assert.GreaterOrEqual(t, startTimes[1].Sub(exitTimes[0]), delay)

	delays, _ := eventsOf[Restarting](sup.rec)
	require.Len(t, delays, 1)
	assert.Equal(t, delay, delays[0].Delay)
}

// TestSignalsDuringStartup pins that signals arriving while the Source
// prepares the child are handled, in order, before the child would start,
// as while no child runs: the reload hook runs and a stop means the child is
// never started.
func TestSignalsDuringStartup(t *testing.T) {
	dir := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	src := fixedSource(helperSpec(t, dir, modeServe), true)

	src.next = func(_ context.Context, n int) (Spec, error) {
		if n == 1 {
			close(entered)
			<-release
		}

		return helperSpec(t, dir, modeServe), nil
	}

	var reloads atomic.Int32

	sup := newHarness(t, src, Options{
		StopSignals:  stopSignals,
		ReloadSignal: syscall.SIGHUP,
		OnReload: func() error {
			reloads.Add(1)

			return nil
		},
	})
	sup.start()

	<-entered
	sup.send(syscall.SIGHUP, syscall.SIGTERM)
	close(release)
	require.NoError(t, sup.wait())

	assert.Equal(t, int32(1), reloads.Load())

	var order []string

	for _, item := range sup.rec.all() {
		switch event := item.event.(type) {
		case Started:
			order = append(order, "started")
		case SignalReceived:
			order = append(order, event.Signal.String()+" "+event.Action.String())
		case Exit:
			order = append(order, "exit")
		}
	}

	assert.Equal(t, []string{
		syscall.SIGHUP.String() + " reload",
		syscall.SIGTERM.String() + " stop",
	}, order)

	nexts, restarts := src.calls()
	assert.Equal(t, 1, nexts)
	assert.Zero(t, restarts)
	sup.assertNoChildren()
}

// TestStopDuringRestartDelay pins that a stop signal or a cancelled context
// ends the restart delay at once and no child is started again.
func TestStopDuringRestartDelay(t *testing.T) {
	for _, viaSignal := range []bool{true, false} {
		t.Run("signal="+strconv.FormatBool(viaSignal), func(t *testing.T) {
			dir := t.TempDir()
			src := fixedSource(helperSpec(t, dir, modeExit, codeEnv+"=1"), true)
			sup := newHarness(t, src, Options{StopSignals: stopSignals, RestartDelay: time.Hour})
			sup.start()

			waitEvent[Restarting](t, sup.rec, 1)

			if viaSignal {
				sup.send(syscall.SIGINT)
			} else {
				sup.cancel()
			}

			require.NoError(t, sup.wait())

			nexts, restarts := src.calls()
			assert.Equal(t, 1, nexts)
			assert.Equal(t, 1, restarts)
			sup.assertNoChildren()

			if viaSignal {
				stop := waitEvent[SignalReceived](t, sup.rec, 1)
				assert.Equal(t, ActionStop, stop.Action)
				assert.Equal(t, syscall.SIGINT, stop.Signal)
			}
		})
	}
}

// TestSignalsDuringRestartDelay pins the handling of signals while no child
// runs: the reload hook runs, anything to forward is dropped.
func TestSignalsDuringRestartDelay(t *testing.T) {
	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeExit, codeEnv+"=1"), true)

	var reloads atomic.Int32

	sup := newHarness(t, src, Options{
		StopSignals:  stopSignals,
		RestartDelay: time.Hour,
		ReloadSignal: syscall.SIGHUP,
		OnReload: func() error {
			reloads.Add(1)

			return errTest
		},
	})
	sup.start()

	waitEvent[Restarting](t, sup.rec, 1)
	sup.send(syscall.SIGUSR1, syscall.SIGHUP, syscall.SIGTERM)
	require.NoError(t, sup.wait())

	got, _ := eventsOf[SignalReceived](sup.rec)
	require.Len(t, got, 3)
	assert.Equal(t, SignalReceived{Signal: syscall.SIGUSR1, Action: ActionDrop}, got[0])
	assert.Equal(t, SignalReceived{Signal: syscall.SIGHUP, Action: ActionReload, Err: errTest},
		got[1])
	assert.Equal(t, SignalReceived{Signal: syscall.SIGTERM, Action: ActionStop}, got[2])
	assert.Equal(t, int32(1), reloads.Load())

	nexts, _ := src.calls()
	assert.Equal(t, 1, nexts)
}

// TestForwardSignals pins that the reload signal runs the hook and reaches
// the child, and that any other signal reaches the child.
func TestForwardSignals(t *testing.T) {
	dir := t.TempDir()

	var reloads atomic.Int32

	sup := newHarness(t, fixedSource(helperSpec(t, dir, modeServe), false), Options{
		StopSignals:  stopSignals,
		ReloadSignal: syscall.SIGHUP,
		OnReload: func() error {
			reloads.Add(1)

			return nil
		},
	})
	sup.start()

	started := waitEvent[Started](t, sup.rec, 1)
	waitReady(t, dir, started.Pid)
	sup.send(syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	waitSignals(t, dir, started.Pid, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	assert.Equal(t, int32(1), reloads.Load())

	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())

	got, _ := eventsOf[SignalReceived](sup.rec)
	require.Len(t, got, 4)
	assert.Equal(t, SignalReceived{Signal: syscall.SIGHUP, Action: ActionReload}, got[0])
	assert.Equal(t, SignalReceived{Signal: syscall.SIGUSR1, Action: ActionForward}, got[1])
	assert.Equal(t, SignalReceived{Signal: syscall.SIGUSR2, Action: ActionForward}, got[2])
}

// TestCheckFailureKillsChild pins that a failed check kills the child, even
// one that ignores the stop signals, and ends Run with the check's error
// instead of a restart.
func TestCheckFailureKillsChild(t *testing.T) {
	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeStubborn), true)

	var checks atomic.Int32

	sup := newHarness(t, src, Options{
		StopSignals: stopSignals,
		CheckPeriod: 50 * time.Millisecond,
		Check: func(context.Context) error {
			if checks.Add(1) < 3 {
				return nil
			}

			return errTest
		},
	})
	sup.start()

	err := sup.wait()
	require.ErrorIs(t, err, errTest)
	assert.Equal(t, OpCheck, errorOp(t, err))

	results, _ := eventsOf[Checked](sup.rec)
	assert.Equal(t, []Checked{{}, {}, {Err: errTest}}, results)

	kills, _ := eventsOf[Killed](sup.rec)
	require.Len(t, kills, 1)
	assert.Equal(t, KillCheckFailed, kills[0].Reason)

	exits, _ := eventsOf[Exit](sup.rec)
	require.Len(t, exits, 1)
	assert.Equal(t, syscall.SIGKILL, termSignal(exits[0].State))

	nexts, restarts := src.calls()
	assert.Equal(t, 1, nexts)
	assert.Zero(t, restarts)
	assert.Equal(t, int32(3), checks.Load())
	sup.assertNoChildren()
}

// TestChecksFollowTheChild pins that checks run while a child runs and never
// while none does, the restart delay included.
func TestChecksFollowTheChild(t *testing.T) {
	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeServe), true)

	var (
		running         atomic.Bool
		checks, strayed atomic.Int32
	)

	rec := &recorder{}
	sup := newHarness(t, src, Options{
		StopSignals:  stopSignals,
		RestartDelay: 300 * time.Millisecond,
		CheckPeriod:  10 * time.Millisecond,
		Check: func(context.Context) error {
			checks.Add(1)

			if !running.Load() {
				strayed.Add(1)
			}

			return nil
		},
		OnEvent: func(event Event) {
			switch event.(type) {
			case Started:
				running.Store(true)
			case Exit:
				running.Store(false)
			}

			rec.record(event)
		},
	})

	sup.rec = rec
	sup.start()

	first := waitEvent[Started](t, rec, 1)
	waitEvent[Checked](t, rec, 3)
	require.NoError(t, syscall.Kill(first.Pid, syscall.SIGKILL))

	second := waitEvent[Started](t, rec, 2)
	waitReady(t, dir, second.Pid)
	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())

	assert.GreaterOrEqual(t, checks.Load(), int32(3))
	assert.Zero(t, strayed.Load(), "checks ran while no child was running")
}

// TestPidFiles pins the pid file lifecycle: the supervisor's for the whole
// run, the child's for each child, both in the format tt reads, and neither
// left behind.
func TestPidFiles(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "run", "supervisor.pid")
	childPidFile := filepath.Join(dir, "run", "child.pid")

	// What the pid files held when the Source was asked for a Spec, when a
	// child was reported started and when it was reported exited.
	var pidFileAtNext, childPidFileAtStart, childPidFileAtExit []string

	pidOrNone := func(path string) string {
		data, err := os.ReadFile(path)
		if err != nil {
			return "none"
		}

		return string(data)
	}
	spec := helperSpec(t, dir, modeServe)
	src := &testSource{
		next: func(context.Context, int) (Spec, error) {
			pidFileAtNext = append(pidFileAtNext, pidOrNone(pidFile))

			return spec, nil
		},
		restart: func(n int, _ Exit) (bool, error) { return n < 2, nil },
	}
	rec := &recorder{}
	sup := newHarness(t, src, Options{
		PidFile:      pidFile,
		ChildPidFile: childPidFile,
		StopSignals:  stopSignals,
		OnEvent: func(event Event) {
			switch event.(type) {
			case Started:
				childPidFileAtStart = append(childPidFileAtStart, pidOrNone(childPidFile))
			case Exit:
				childPidFileAtExit = append(childPidFileAtExit, pidOrNone(childPidFile))
			}

			rec.record(event)
		},
	})

	sup.rec = rec
	sup.start()

	first := waitEvent[Started](t, rec, 1)
	assert.Equal(t, os.Getpid(), readPid(t, pidFile))
	assert.Equal(t, first.Pid, readPid(t, childPidFile))

	info, err := os.Stat(pidFile)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	waitReady(t, dir, first.Pid)
	require.NoError(t, syscall.Kill(first.Pid, syscall.SIGKILL))

	second := waitEvent[Started](t, rec, 2)
	assert.Equal(t, second.Pid, readPid(t, childPidFile))
	assert.Equal(t, os.Getpid(), readPid(t, pidFile))

	waitReady(t, dir, second.Pid)
	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())

	assert.NoFileExists(t, pidFile)
	assert.NoFileExists(t, childPidFile)

	self := strconv.Itoa(os.Getpid())
	assert.Equal(t, []string{self, self}, pidFileAtNext)
	assert.Equal(t, []string{strconv.Itoa(first.Pid), strconv.Itoa(second.Pid)},
		childPidFileAtStart)
	assert.Equal(t, []string{"none", "none"}, childPidFileAtExit)

	written, _ := eventsOf[PidFileWritten](rec)
	assert.Equal(t, []PidFileWritten{
		{Path: pidFile, Pid: os.Getpid()},
		{Path: childPidFile, Pid: first.Pid},
		{Path: childPidFile, Pid: second.Pid},
	}, written)
}

// TestPidFileOfLiveProcess pins that the pid file of a live process is
// refused and left alone, before anything runs.
func TestPidFileOfLiveProcess(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "supervisor.pid")
	other := strconv.Itoa(os.Getppid())
	require.NoError(t, os.WriteFile(pidFile, []byte(other), 0o600))

	var cleanups atomic.Int32

	src := fixedSource(helperSpec(t, dir, modeServe), false)
	sup := newHarness(t, src, Options{
		PidFile:     pidFile,
		StopSignals: stopSignals,
		Cleanup:     func() { cleanups.Add(1) },
	})
	sup.start()

	err := sup.wait()
	require.Error(t, err)
	assert.Equal(t, OpPidFile, errorOp(t, err))

	nexts, _ := src.calls()
	assert.Zero(t, nexts)
	assert.Zero(t, cleanups.Load())

	data, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	assert.Equal(t, other, string(data))
}

// TestStalePidFile pins that a pid file naming a dead process is replaced.
func TestStalePidFile(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "supervisor.pid")
	require.NoError(t, os.WriteFile(pidFile, []byte(strconv.Itoa(deadPid(t))), 0o600))

	sup := newHarness(t, fixedSource(helperSpec(t, dir, modeServe), false), Options{
		PidFile:     pidFile,
		StopSignals: stopSignals,
	})
	sup.start()

	started := waitEvent[Started](t, sup.rec, 1)
	assert.Equal(t, os.Getpid(), readPid(t, pidFile))

	waitReady(t, dir, started.Pid)
	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())
	assert.NoFileExists(t, pidFile)
}

// TestCleanupOnEveryExit pins, for every way Run can end after it created
// its pid file, that the child is gone before Cleanup runs, Cleanup runs
// once and before the supervisor pid file goes, and no pid file of ours is
// left.
func TestCleanupOnEveryExit(t *testing.T) {
	type setup struct {
		dir      string
		spec     Spec
		src      *testSource
		opts     Options
		childPid string
	}

	cases := []struct {
		name    string
		prepare func(t *testing.T, state *setup)
		act     func(t *testing.T, sup *harness, state *setup)
		op      Op
	}{
		{
			name: "stop signal",
			act: func(t *testing.T, sup *harness, state *setup) {
				t.Helper()

				started := waitEvent[Started](t, sup.rec, 1)
				waitReady(t, state.dir, started.Pid)
				sup.send(syscall.SIGTERM)
			},
		},
		{
			name: "cancelled context",
			act: func(t *testing.T, sup *harness, state *setup) {
				t.Helper()

				started := waitEvent[Started](t, sup.rec, 1)
				waitReady(t, state.dir, started.Pid)
				sup.cancel()
			},
		},
		{
			name: "no restart",
			prepare: func(t *testing.T, state *setup) {
				t.Helper()

				state.spec = helperSpec(t, state.dir, modeExit, codeEnv+"=0")
			},
		},
		{
			name: "stop during restart delay",
			prepare: func(t *testing.T, state *setup) {
				t.Helper()

				state.spec = helperSpec(t, state.dir, modeExit, codeEnv+"=1")
				state.src.restart = func(int, Exit) (bool, error) { return true, nil }
				state.opts.RestartDelay = time.Hour
			},
			act: func(t *testing.T, sup *harness, _ *setup) {
				t.Helper()

				waitEvent[Restarting](t, sup.rec, 1)
				sup.send(syscall.SIGINT)
			},
		},
		{
			name: "source fails",
			prepare: func(_ *testing.T, state *setup) {
				state.src.next = func(context.Context, int) (Spec, error) { return Spec{}, errTest }
			},
			op: OpNext,
		},
		{
			name: "invalid spec",
			prepare: func(_ *testing.T, state *setup) {
				state.spec = Spec{}
			},
			op: OpNext,
		},
		{
			name: "start fails",
			prepare: func(_ *testing.T, state *setup) {
				state.spec.Path = filepath.Join(state.dir, "missing")
			},
			op: OpStart,
		},
		{
			name: "restart decision fails",
			prepare: func(t *testing.T, state *setup) {
				t.Helper()

				state.spec = helperSpec(t, state.dir, modeExit, codeEnv+"=1")
				state.src.restart = func(int, Exit) (bool, error) { return false, errTest }
			},
			op: OpRestart,
		},
		{
			name: "check fails",
			prepare: func(t *testing.T, state *setup) {
				t.Helper()

				state.spec = helperSpec(t, state.dir, modeStubborn)
				state.opts.CheckPeriod = 20 * time.Millisecond
				state.opts.Check = func(context.Context) error { return errTest }
			},
			op: OpCheck,
		},
		{
			name: "child pid file refused",
			prepare: func(t *testing.T, state *setup) {
				t.Helper()

				state.childPid = strconv.Itoa(os.Getppid())
				require.NoError(t,
					os.WriteFile(state.opts.ChildPidFile, []byte(state.childPid), 0o600))
			},
			op: OpChildPidFile,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			state := &setup{
				dir:  dir,
				spec: helperSpec(t, dir, modeServe),
				opts: Options{
					PidFile:      filepath.Join(dir, "supervisor.pid"),
					ChildPidFile: filepath.Join(dir, "child.pid"),
					StopSignals:  stopSignals,
				},
			}

			state.src = &testSource{restart: func(int, Exit) (bool, error) { return false, nil }}
			state.src.next = func(context.Context, int) (Spec, error) { return state.spec, nil }

			if test.prepare != nil {
				test.prepare(t, state)
			}

			var (
				cleanups         atomic.Int32
				pidFileAtCleanup bool
				sup              *harness
			)

			state.opts.Cleanup = func() {
				cleanups.Add(1)

				_, err := os.Stat(state.opts.PidFile)

				pidFileAtCleanup = err == nil

				sup.assertNoChildren()
			}
			sup = newHarness(t, state.src, state.opts)
			sup.start()

			if test.act != nil {
				test.act(t, sup, state)
			}

			err := sup.wait()
			if test.op == 0 {
				require.NoError(t, err)
			} else {
				assert.Equal(t, test.op, errorOp(t, err))
			}

			assert.Equal(t, int32(1), cleanups.Load())
			assert.True(t, pidFileAtCleanup, "the supervisor pid file went before Cleanup")
			assert.NoFileExists(t, state.opts.PidFile)
			sup.assertNoChildren()

			if state.childPid == "" {
				assert.NoFileExists(t, state.opts.ChildPidFile)
			} else {
				data, err := os.ReadFile(state.opts.ChildPidFile)
				require.NoError(t, err)
				assert.Equal(t, state.childPid, string(data), "a pid file of another process")

				kills, _ := eventsOf[Killed](sup.rec)
				require.Len(t, kills, 1)
				assert.Equal(t, KillChildPidFile, kills[0].Reason)
			}
		})
	}
}

// TestProcessGroup pins that with ProcessGroup the child leads its own
// group, signals reach the whole group, and the stop timeout kills the whole
// group; without it the child stays in the supervisor's group.
func TestProcessGroup(t *testing.T) {
	t.Run("own group", func(t *testing.T) {
		dir := t.TempDir()
		spec := helperSpec(t, dir, modeFamily)

		spec.ProcessGroup = true
		spec.StopTimeout = 300 * time.Millisecond

		sup := newHarness(t, fixedSource(spec, true), Options{StopSignals: stopSignals})
		sup.start()

		started := waitEvent[Started](t, sup.rec, 1)
		grandchild, err := strconv.Atoi(waitFile(t, filepath.Join(dir, "grandchild")))
		require.NoError(t, err)
		t.Cleanup(func() { _ = syscall.Kill(grandchild, syscall.SIGKILL) })

		waitReady(t, dir, started.Pid)
		waitReady(t, dir, grandchild)

		pgid, err := syscall.Getpgid(started.Pid)
		require.NoError(t, err)
		assert.Equal(t, started.Pid, pgid)

		sup.send(syscall.SIGUSR1)
		waitSignals(t, dir, started.Pid, syscall.SIGUSR1)
		waitSignals(t, dir, grandchild, syscall.SIGUSR1)

		sup.send(syscall.SIGTERM)
		require.NoError(t, sup.wait())

		kills, _ := eventsOf[Killed](sup.rec)
		require.Len(t, kills, 1)
		assert.Equal(t, KillStopTimeout, kills[0].Reason)
		// The grandchild is not ours to wait for; its new parent reaps it.
		assert.Eventually(t, func() bool {
			return syscall.Kill(grandchild, 0) == syscall.ESRCH
		}, waitTimeout, pollInterval, "the grandchild survived the group kill")
	})

	t.Run("shared group", func(t *testing.T) {
		dir := t.TempDir()
		sup := newHarness(t, fixedSource(helperSpec(t, dir, modeServe), true),
			Options{StopSignals: stopSignals})
		sup.start()

		started := waitEvent[Started](t, sup.rec, 1)
		pgid, err := syscall.Getpgid(started.Pid)
		require.NoError(t, err)
		assert.Equal(t, syscall.Getpgrp(), pgid)

		waitReady(t, dir, started.Pid)
		sup.send(syscall.SIGTERM)
		require.NoError(t, sup.wait())
	})
}

// TestStartFunc pins that every child is started through Options.Start, with
// the command prepared from the Spec, and that the StartFunc may change what
// runs.
func TestStartFunc(t *testing.T) {
	dir := t.TempDir()
	exe, err := os.Executable()
	require.NoError(t, err)

	spec := helperSpec(t, dir, modeExit, codeEnv+"=0")

	spec.Path = filepath.Join(dir, "not-there")
	spec.Args = []string{"one", "two"}
	spec.Dir = dir
	spec.ProcessGroup = true

	var cmds []*exec.Cmd

	src := fixedSource(spec, true)

	src.restart = func(n int, _ Exit) (bool, error) { return n < 2, nil }

	sup := newHarness(t, src, Options{
		StopSignals: stopSignals,
		Start: func(cmd *exec.Cmd) error {
			cmds = append(cmds, cmd)
			cmd.Path = exe

			return StartCmd(cmd)
		},
	})
	sup.start()
	require.NoError(t, sup.wait())

	require.Len(t, cmds, 2)

	for _, cmd := range cmds {
		assert.Equal(t, []string{spec.Path, "one", "two"}, cmd.Args)
		assert.Equal(t, dir, cmd.Dir)
		assert.Equal(t, spec.Env, cmd.Env)
		assert.True(t, cmd.SysProcAttr.Setpgid)
	}

	exits, _ := eventsOf[Exit](sup.rec)
	require.Len(t, exits, 2)

	for _, exit := range exits {
		assert.True(t, exit.State.Success())
	}
}

// TestStartFuncWithoutProcess pins that a StartFunc reporting success
// without starting anything is an error, not a crash.
func TestStartFuncWithoutProcess(t *testing.T) {
	dir := t.TempDir()
	sup := newHarness(t, fixedSource(helperSpec(t, dir, modeServe), false), Options{
		Start: func(*exec.Cmd) error { return nil },
	})
	sup.start()

	err := sup.wait()
	require.ErrorIs(t, err, ErrNotStarted)
	assert.Equal(t, OpStart, errorOp(t, err))
}

// TestStdin pins that Spec.Stdin reaches the child's standard input and
// that without it the child reads an empty one.
func TestStdin(t *testing.T) {
	for _, input := range [][]byte{[]byte("local launcher = true\n"), nil} {
		dir := t.TempDir()
		spec := helperSpec(t, dir, modeServe, stdinEnv+"=1")

		spec.Stdin = input

		sup := newHarness(t, fixedSource(spec, false), Options{StopSignals: stopSignals})
		sup.start()

		started := waitEvent[Started](t, sup.rec, 1)
		waitReady(t, dir, started.Pid)

		data, err := os.ReadFile(helperFile(dir, "stdin", started.Pid))
		require.NoError(t, err)
		assert.Equal(t, string(input), string(data))

		sup.send(syscall.SIGTERM)
		require.NoError(t, sup.wait())
	}
}

// TestNewRefusesOptions pins the option combinations New refuses.
func TestNewRefusesOptions(t *testing.T) {
	check := func(context.Context) error { return nil }
	reload := func() error { return nil }
	src := fixedSource(Spec{}, false)

	cases := map[string]Options{
		"SIGKILL stops":       {StopSignals: []syscall.Signal{syscall.SIGKILL}},
		"SIGCHLD stops":       {StopSignals: []syscall.Signal{syscall.SIGCHLD}},
		"zero stop signal":    {StopSignals: []syscall.Signal{0}},
		"reload without hook": {ReloadSignal: syscall.SIGHUP},
		"hook without reload": {OnReload: reload},
		"SIGURG reloads":      {ReloadSignal: syscall.SIGURG, OnReload: reload},
		"reload is a stop": {
			StopSignals: stopSignals, ReloadSignal: syscall.SIGINT, OnReload: reload,
		},
		"negative delay":       {RestartDelay: -time.Second},
		"check without period": {Check: check},
		"period without check": {CheckPeriod: time.Second},
		"negative period":      {CheckPeriod: -time.Second, Check: check},
		"SIGKILL ignored":      {IgnoreSignals: []syscall.Signal{syscall.SIGKILL}},
		"ignored stop": {
			StopSignals: stopSignals, IgnoreSignals: []syscall.Signal{syscall.SIGTERM},
		},
		"ignored reload": {
			ReloadSignal: syscall.SIGHUP, OnReload: reload,
			IgnoreSignals: []syscall.Signal{syscall.SIGHUP},
		},
	}

	for name, opts := range cases {
		_, err := New(src, opts)
		require.ErrorIs(t, err, ErrInvalidOptions, name)
	}

	_, err := New(nil, Options{})
	require.ErrorIs(t, err, ErrInvalidOptions)
}

// TestRunOnce pins that an engine runs once.
func TestRunOnce(t *testing.T) {
	dir := t.TempDir()
	sup := newHarness(t, fixedSource(helperSpec(t, dir, modeExit, codeEnv+"=0"), false), Options{})
	sup.start()
	require.NoError(t, sup.wait())
	require.ErrorIs(t, sup.engine.Run(t.Context()), ErrAlreadyRun)
}

// TestDetach pins that a detached process leads its own process group.
func TestDetach(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(helperEnv, modeStubborn)
	t.Setenv(dirEnv, dir)
	t.Setenv("GORACE", "atexit_sleep_ms=0")

	exe, err := os.Executable()
	require.NoError(t, err)

	pid, err := Detach(exe, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

	waitReady(t, dir, pid)

	pgid, err := syscall.Getpgid(pid)
	require.NoError(t, err)
	assert.Equal(t, pid, pgid)

	// Once it exits, Detach has waited for it: no zombie is left behind in a
	// caller that lives on.
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	assertReaped(t, pid)

	_, err = Detach(exe, nil, func(*exec.Cmd) error { return errTest })
	require.ErrorIs(t, err, errTest)
}

// TestDetachReaps pins that a detached process that exits on its own is
// waited for.
func TestDetachReaps(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(helperEnv, modeExit)
	t.Setenv(dirEnv, dir)
	t.Setenv(codeEnv, "0")
	t.Setenv("GORACE", "atexit_sleep_ms=0")

	exe, err := os.Executable()
	require.NoError(t, err)

	pid, err := Detach(exe, nil, nil)
	require.NoError(t, err)
	assertReaped(t, pid)
}

// assertReaped waits until pid is gone for good. A zombie still answers
// signal 0; only a waited-for process does not.
func assertReaped(t *testing.T, pid int) {
	t.Helper()

	assert.Eventually(t, func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	}, waitTimeout, pollInterval, "%d was not waited for", pid)
}

// TestSubscribeOS pins that the real subscription relays signals but not
// the runtime's SIGURG or SIGCHLD.
func TestSubscribeOS(t *testing.T) {
	signals, unsubscribe := subscribeOS()
	defer unsubscribe()

	self := os.Getpid()
	require.NoError(t, syscall.Kill(self, syscall.SIGURG))
	require.NoError(t, syscall.Kill(self, syscall.SIGCHLD))
	require.NoError(t, syscall.Kill(self, syscall.SIGUSR1))

	select {
	case sig := <-signals:
		assert.Equal(t, syscall.SIGUSR1, sig)
	case <-time.After(waitTimeout):
		require.FailNow(t, "no signal relayed")
	}

	select {
	case sig := <-signals:
		assert.Failf(t, "unexpected signal", "%v", sig)
	case <-time.After(100 * time.Millisecond):
	}
}
