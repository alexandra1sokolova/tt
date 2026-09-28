package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSignalStormAcrossRestarts sends bursts of signals from several
// goroutines, round after round, alternately while a child runs and while
// none does. It pins that every signal the engine receives is handled
// exactly once and in its right place: forwarded, with the reload hook run
// for each reload signal, while a child runs, and then only to that child;
// dropped, reload hook still run, while none runs; and a stop, sent last,
// still ends Run.
//
// The phases do not depend on timing. In a running phase the child stays up
// until the test kills it after the burst is handled; in an idle phase the
// Source holds the next start until the burst is sent, and the engine takes
// the signals that came before the start as with no child.
//
// Injected, every signal sent reaches the engine, so the counts are exact.
// Through the relay the engine runs on, a signal is sent the way the Go
// runtime delivers one, dropped when the relay's input is full, with the
// runtime's own SIGURG and SIGCHLD mixed in; then the counts are only
// bounded by what was accepted.
func TestSignalStormAcrossRestarts(t *testing.T) {
	t.Run("injected", func(t *testing.T) { signalStorm(t, false) })
	t.Run("relay", func(t *testing.T) { signalStorm(t, true) })
}

func signalStorm(t *testing.T, viaRelay bool) {
	t.Helper()

	const (
		rounds  = 3
		senders = 8
		// perSender keeps a burst below the buffers of the relay, so that
		// only its input, not its output, can drop a signal.
		perSender = 12
		reloadsIn = 4 // One signal in reloadsIn is the reload signal.
		// Per burst: the reload signals, and the signals to forward.
		burstReloads  = senders * ((perSender + reloadsIn - 1) / reloadsIn)
		burstForwards = senders*perSender - burstReloads
	)

	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeServe), true)
	entered := make(chan struct{})
	release := make(chan struct{})

	// Every start but the first waits for the idle burst to be sent.
	src.next = func(ctx context.Context, n int) (Spec, error) {
		if n > 1 {
			entered <- struct{}{}

			select {
			case <-release:
			case <-ctx.Done():
			}
		}

		return helperSpec(t, dir, modeServe), nil
	}

	var reloads, accepted atomic.Int32

	sup := newHarness(t, src, Options{
		StopSignals:  stopSignals,
		ReloadSignal: syscall.SIGHUP,
		OnReload: func() error {
			reloads.Add(1)

			return nil
		},
	})

	send := func(sig syscall.Signal) {
		sup.send(sig)
		accepted.Add(1)
	}

	// queued is what waits for the engine to read it.
	queued := func() int { return len(sup.signals) }

	if viaRelay {
		raw := make(chan os.Signal, signalBuffer)
		relayed := make(chan (<-chan os.Signal), 1)

		sup.engine.subscribe = func() (<-chan os.Signal, func()) {
			out, stop := relay(raw, func() {})
			relayed <- out

			return out, stop
		}
		queued = func() int {
			out := <-relayed
			relayed <- out

			return len(out)
		}
		// Like the runtime: never block, drop what does not fit.
		send = func(sig syscall.Signal) {
			select {
			case raw <- sig:
				if sig != syscall.SIGURG && sig != syscall.SIGCHLD {
					accepted.Add(1)
				}
			default:
			}
		}
	}

	burst := func() {
		var group sync.WaitGroup

		for range senders {
			group.Go(func() {
				for index := range perSender {
					sig := syscall.SIGUSR1
					if index%reloadsIn == 0 {
						sig = syscall.SIGHUP
					}

					send(sig)

					if viaRelay && index%reloadsIn == 1 {
						send(syscall.SIGURG)
						send(syscall.SIGCHLD)
					}
				}
			})
		}

		group.Wait()
	}

	waitHandled := func() {
		require.Eventually(t, func() bool {
			handled, _ := eventsOf[SignalReceived](sup.rec)

			return len(handled) == int(accepted.Load())
		}, waitTimeout, pollInterval, "the burst was not handled")
	}

	sup.start()

	for round := 1; round <= rounds; round++ {
		started := waitEvent[Started](t, sup.rec, round)
		waitReady(t, dir, started.Pid)

		burst()
		waitHandled()

		require.NoError(t, syscall.Kill(started.Pid, syscall.SIGKILL))
		<-entered

		before := accepted.Load()

		burst()

		// The whole burst has to wait for the engine, not be on its way
		// through the relay, when the start goes ahead: a signal that
		// arrives while the child starts goes to the child.
		require.Eventually(t, func() bool {
			return queued() == int(accepted.Load()-before)
		}, waitTimeout, pollInterval, "the burst did not reach the engine")

		release <- struct{}{}

		waitHandled()
	}

	last := waitEvent[Started](t, sup.rec, rounds+1)
	waitReady(t, dir, last.Pid)

	// A stop sent through the relay can be dropped as well: resend it
	// until Run returns, as a user would.
	for stopped := false; !stopped; {
		send(syscall.SIGTERM)

		select {
		case err := <-sup.done:
			sup.done <- err

			stopped = true
		case <-time.After(10 * time.Millisecond):
		}
	}

	require.NoError(t, sup.wait())

	var (
		running bool
		current int
		actions = map[SignalAction]int{}
	)

	for _, item := range sup.rec.all() {
		switch event := item.event.(type) {
		case Started:
			running, current = true, event.Pid
		case Exit:
			require.Equal(t, current, event.Pid)

			running = false
		case SignalReceived:
			actions[event.Action]++

			require.NotContains(t, []syscall.Signal{syscall.SIGURG, syscall.SIGCHLD},
				event.Signal, "the relay passed a runtime signal on")

			switch event.Action {
			case ActionForward:
				require.True(t, running, "a signal forwarded while no child ran")
				require.NoError(t, event.Err, "the child to forward to was gone")
			case ActionDrop:
				require.False(t, running, "a signal dropped while a child ran")
				require.NoError(t, event.Err)
			case ActionReload, ActionStop, ActionIgnore:
			}
		}
	}

	forwards, drops := actions[ActionForward], actions[ActionDrop]
	handled := forwards + drops + actions[ActionReload] + actions[ActionStop]
	assert.Equal(t, actions[ActionReload], int(reloads.Load()))
	assert.Equal(t, int(accepted.Load()), handled)

	if viaRelay {
		assert.GreaterOrEqual(t, actions[ActionStop], 1)
		// Both phases have to be there for the test to test anything.
		assert.Positive(t, forwards)
		assert.Positive(t, drops)
	} else {
		assert.Equal(t, 1, actions[ActionStop])
		assert.Equal(t, rounds*burstForwards, forwards)
		assert.Equal(t, rounds*burstForwards, drops)
		assert.Equal(t, 2*rounds*burstReloads, actions[ActionReload])
	}

	nexts, restarts := src.calls()
	assert.Equal(t, rounds+1, nexts)
	assert.Equal(t, rounds, restarts)
	t.Logf("%d starts, %d accepted, %d handled: %d forwarded, %d dropped",
		nexts, accepted.Load(), handled, forwards, drops)
	sup.assertNoChildren()
}

// TestStopRacesRestart stops the engine at a different moment in each round
// while a child keeps exiting and restarting, with signals arriving all
// along. Whenever the
// stop lands - while the child starts, runs, exits or waits for its restart -
// Run returns without another start after it, the child is gone and waited
// for, Cleanup has run once and no pid file is left.
func TestStopRacesRestart(t *testing.T) {
	const (
		rounds = 24
		// stopSpread is the step, in milliseconds, between the stop times
		// of consecutive rounds; the child cycles in about 10ms.
		stopSpread = 7
	)

	for round := range rounds {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "supervisor.pid")
			childPidFile := filepath.Join(dir, "child.pid")
			src := fixedSource(helperSpec(t, dir, modeExit, codeEnv+"=1"), true)

			var cleanups atomic.Int32

			sup := newHarness(t, src, Options{
				PidFile:      pidFile,
				ChildPidFile: childPidFile,
				StopSignals:  stopSignals,
				RestartDelay: time.Duration(round%5) * time.Millisecond,
				Cleanup:      func() { cleanups.Add(1) },
			})
			sup.start()

			noise, stopNoise := context.WithCancel(context.Background())
			defer stopNoise()

			go func() {
				for noise.Err() == nil {
					sup.send(syscall.SIGUSR2)
					time.Sleep(time.Millisecond)
				}
			}()

			// Spread the stops over the whole cycle of start, run, exit
			// and delay.
			time.Sleep(time.Duration(round*stopSpread) * time.Millisecond)

			stopAt := time.Now()

			if round%2 == 0 {
				sup.send(syscall.SIGTERM)
			} else {
				sup.cancel()
			}

			require.NoError(t, sup.wait())
			stopNoise()

			_, startTimes := eventsOf[Started](sup.rec)
			late := 0

			for _, at := range startTimes {
				if at.After(stopAt) {
					late++
				}
			}

			// A stop that lands while the child is being started lets that
			// one start happen and stops it; no other start may follow.
			assert.LessOrEqual(t, late, 1, "children started after the stop")
			assert.Equal(t, int32(1), cleanups.Load())
			assert.NoFileExists(t, pidFile)
			assert.NoFileExists(t, childPidFile)
			sup.assertNoChildren()
		})
	}
}

// TestRealSignals runs the engine in a separate process that takes real
// signals: it pins the OS subscription together with the pid files, the
// reload hook, forwarding and a stop, end to end.
func TestRealSignals(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "supervisor.pid")
	childPidFile := filepath.Join(dir, "child.pid")

	exe, err := os.Executable()
	require.NoError(t, err)

	cmd := command(t.Context(), &Spec{
		Path: exe,
		Env: append(os.Environ(),
			helperEnv+"="+modeSupervise,
			dirEnv+"="+dir,
			pidFileEnv+"="+pidFile,
			childPidFileEnv+"="+childPidFile,
			"GORACE=atexit_sleep_ms=0"),
		StopSignal:  syscall.SIGTERM,
		StopTimeout: longStopTimeout,
	})
	require.NoError(t, cmd.Start())

	supervisor := cmd.Process.Pid
	exited := make(chan error, 1)

	go func() { exited <- cmd.Wait() }()

	childPid := waitPid(t, childPidFile)

	t.Cleanup(func() {
		_ = syscall.Kill(supervisor, syscall.SIGKILL)
		_ = syscall.Kill(childPid, syscall.SIGKILL)

		<-exited
	})

	assert.Equal(t, supervisor, waitPid(t, pidFile))
	waitReady(t, dir, childPid)

	require.NoError(t, syscall.Kill(supervisor, syscall.SIGHUP))
	waitFile(t, filepath.Join(dir, "reloaded"))
	waitSignals(t, dir, childPid, syscall.SIGHUP)

	require.NoError(t, syscall.Kill(supervisor, syscall.SIGUSR1))
	waitSignals(t, dir, childPid, syscall.SIGUSR1)

	require.NoError(t, syscall.Kill(supervisor, syscall.SIGINT))

	select {
	case err := <-exited:
		exited <- err

		require.NoError(t, err)
	case <-time.After(waitTimeout):
		require.FailNow(t, "the supervisor did not exit")
	}

	assert.Equal(t, strconv.Itoa(exitOnInt), waitFile(t, filepath.Join(dir, "exit")))
	assert.NoFileExists(t, pidFile)
	assert.NoFileExists(t, childPidFile)
	assert.ErrorIs(t, syscall.Kill(childPid, 0), syscall.ESRCH)
}

// waitPid waits for a pid file to hold a pid and returns it.
func waitPid(t *testing.T, path string) int {
	t.Helper()

	var pid int

	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}

		pid, err = strconv.Atoi(string(data))

		return err == nil
	}, waitTimeout, pollInterval, "%s holds no pid", path)

	return pid
}
