package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkWhileChildExits returns a Check that, on its first run, kills the
// child and then waits until the engine has seen the exit and cancelled it,
// and only then returns what finish makes of its context. Every later run
// passes. The child pid comes from the Started events.
func checkWhileChildExits(child *atomic.Int64, finish func(ctx context.Context) error,
) func(ctx context.Context) error {
	var runs atomic.Int32

	return func(ctx context.Context) error {
		if runs.Add(1) > 1 {
			return nil
		}

		_ = syscall.Kill(int(child.Load()), syscall.SIGKILL)

		<-ctx.Done()

		return finish(ctx)
	}
}

// TestCheckFailsAsChildExits pins that a check that fails while the child
// exits is not lost: Run ends with it and the child is not restarted.
func TestCheckFailsAsChildExits(t *testing.T) {
	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeServe), false)

	var child atomic.Int64

	rec := &recorder{}
	sup := newHarness(t, src, Options{
		StopSignals: stopSignals,
		CheckPeriod: 20 * time.Millisecond,
		Check: checkWhileChildExits(&child, func(context.Context) error {
			return errTest
		}),
		OnEvent: func(event Event) {
			started, ok := event.(Started)
			if ok {
				child.Store(int64(started.Pid))
			}

			rec.record(event)
		},
	})

	sup.rec = rec
	sup.start()

	err := sup.wait()
	require.ErrorIs(t, err, errTest)
	assert.Equal(t, OpCheck, errorOp(t, err))

	nexts, restarts := src.calls()
	assert.Equal(t, 1, nexts)
	assert.Zero(t, restarts, "the Source was asked about a restart after a failed check")

	checked, _ := eventsOf[Checked](rec)
	assert.Equal(t, []Checked{{Err: errTest}}, checked)
}

// TestCheckCancelledAsChildExits pins the other side: a check that gives up
// because the child exited under it has not failed, and the Source decides
// on the restart as after any other exit.
func TestCheckCancelledAsChildExits(t *testing.T) {
	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeServe), false)

	var child atomic.Int64

	rec := &recorder{}
	sup := newHarness(t, src, Options{
		StopSignals: stopSignals,
		CheckPeriod: 20 * time.Millisecond,
		Check: checkWhileChildExits(&child, func(ctx context.Context) error {
			return fmt.Errorf("check interrupted: %w", ctx.Err())
		}),
		OnEvent: func(event Event) {
			started, ok := event.(Started)
			if ok {
				child.Store(int64(started.Pid))
			}

			rec.record(event)
		},
	})

	sup.rec = rec
	sup.start()
	require.NoError(t, sup.wait())

	_, restarts := src.calls()
	assert.Equal(t, 1, restarts)

	checked, _ := eventsOf[Checked](rec)
	assert.Empty(t, checked)
}

// TestStopWhileSourcePrepares pins that a stop that arrives while the Source
// prepares the next child is honoured before that child is started.
func TestStopWhileSourcePrepares(t *testing.T) {
	dir := t.TempDir()
	spec := helperSpec(t, dir, modeExit, codeEnv+"=1")
	src := fixedSource(spec, true)

	var sup *harness

	src.next = func(_ context.Context, n int) (Spec, error) {
		if n == 2 {
			sup.send(syscall.SIGTERM)
		}

		return spec, nil
	}

	sup = newHarness(t, src, Options{StopSignals: stopSignals})
	sup.start()
	require.NoError(t, sup.wait())

	started, _ := eventsOf[Started](sup.rec)
	assert.Len(t, started, 1, "a child started after the stop was queued")

	nexts, _ := src.calls()
	assert.Equal(t, 2, nexts)
}

// TestQueuedStopBeatsRestart repeats the scenario where a stop is queued
// while the child exits, or as the restart delay begins, with no delay at
// all: the engine finds the stop ready together with the exit or with the
// expired timer. The stop must win every time: no child may start after it
// was queued. A choice between the two left to chance loses at least one
// round in four, so forty rounds miss it with a chance below 1e-5.
func TestQueuedStopBeatsRestart(t *testing.T) {
	const rounds = 40

	cases := []struct {
		name string
		// queue sends the stop at the moment of the scenario; it returns
		// once both the stop and the competing event are ready.
		queue func(t *testing.T, sup *harness, event Event) bool
	}{
		{
			name: "child exit",
			queue: func(t *testing.T, sup *harness, event Event) bool {
				t.Helper()

				started, ok := event.(Started)
				if !ok {
					return false
				}

				sup.send(syscall.SIGTERM)
				// Once it is reaped, its exit waits for the engine.
				waitReaped(t, started.Pid)

				return true
			},
		},
		{
			name: "restart delay",
			queue: func(t *testing.T, sup *harness, event Event) bool {
				t.Helper()

				_, ok := event.(Restarting)
				if !ok {
					return false
				}

				sup.send(syscall.SIGTERM)

				return true
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			for round := range rounds {
				dir := t.TempDir()
				src := fixedSource(helperSpec(t, dir, modeExit, codeEnv+"=1"), true)
				rec := &recorder{}

				var (
					sup    *harness
					queued atomic.Bool
				)

				sup = newHarness(t, src, Options{
					StopSignals: stopSignals,
					OnEvent: func(event Event) {
						rec.record(event)

						if !queued.Load() && test.queue(t, sup, event) {
							queued.Store(true)
						}
					},
				})

				sup.rec = rec
				sup.start()
				require.NoError(t, sup.wait())

				started, _ := eventsOf[Started](rec)
				require.Len(t, started, 1, "round %d: children started after the stop", round)

				// Nor is the Source asked for anything after the stop: a
				// Spec it failed to prepare would turn a clean stop into an
				// error. A stop queued as the child exits is taken before
				// the Source is asked about a restart.
				nexts, restarts := src.calls()
				require.Equal(t, 1, nexts, "round %d: asked for a Spec after the stop", round)

				if test.name == "child exit" {
					require.Zero(t, restarts, "round %d: asked to restart after the stop", round)
				}
			}
		})
	}
}

// waitReaped waits until pid has been waited for.
func waitReaped(t *testing.T, pid int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	}, waitTimeout, time.Millisecond)
}

// errPanic is what the panicking callbacks panic with.
var errPanic = errors.New("callback panic")

// TestPanicTearsDown pins that a callback panicking out of Run still tears
// the supervision down: the child is killed and waited for, the checks stop,
// Cleanup runs, the pid files go, and the panic reaches the caller.
func TestPanicTearsDown(t *testing.T) {
	cases := []struct {
		name string
		// panics tells whether the callback panics on this event.
		panics func(event Event) bool
		// send is a signal sent once the child runs, 0 for none.
		send syscall.Signal
		// cleanupPanics makes Cleanup itself panic.
		cleanupPanics bool
	}{
		{
			name: "on started",
			panics: func(event Event) bool {
				_, ok := event.(Started)

				return ok
			},
		},
		{
			name: "on a signal",
			panics: func(event Event) bool {
				_, ok := event.(SignalReceived)

				return ok
			},
			send: syscall.SIGUSR1,
		},
		{
			name: "in cleanup",
			panics: func(event Event) bool {
				_, ok := event.(Started)

				return ok
			},
			cleanupPanics: true,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "supervisor.pid")
			childPidFile := filepath.Join(dir, "child.pid")
			src := fixedSource(helperSpec(t, dir, modeStubborn), true)

			var (
				child    atomic.Int64
				checks   atomic.Int32
				cleanups atomic.Int32
			)

			signals := make(chan os.Signal, 1)
			engine, err := New(src, Options{
				PidFile:      pidFile,
				ChildPidFile: childPidFile,
				StopSignals:  stopSignals,
				CheckPeriod:  5 * time.Millisecond,
				Check: func(context.Context) error {
					checks.Add(1)

					return nil
				},
				Cleanup: func() {
					cleanups.Add(1)

					if test.cleanupPanics {
						panic(errPanic)
					}
				},
				OnEvent: func(event Event) {
					started, ok := event.(Started)
					if ok {
						child.Store(int64(started.Pid))

						if test.send != 0 {
							waitReady(t, dir, started.Pid)

							signals <- test.send
						}
					}

					if test.panics(event) {
						panic(errPanic)
					}
				},
			})
			require.NoError(t, err)

			engine.subscribe = func() (<-chan os.Signal, func()) { return signals, func() {} }

			recovered := make(chan any, 1)

			go func() {
				defer func() { recovered <- recover() }()

				_ = engine.Run(context.Background())
			}()

			select {
			case value := <-recovered:
				assert.Equal(t, errPanic, value)
			case <-time.After(waitTimeout):
				require.FailNow(t, "Run neither returned nor panicked")
			}

			pid := int(child.Load())
			require.NotZero(t, pid)
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH,
				"the child was not killed and waited for")
			assert.Equal(t, int32(1), cleanups.Load())
			assert.NoFileExists(t, pidFile)
			assert.NoFileExists(t, childPidFile)

			stopped := checks.Load()

			time.Sleep(50 * time.Millisecond)
			assert.Equal(t, stopped, checks.Load(), "the checks outlived Run")
		})
	}
}
