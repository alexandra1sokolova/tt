package supervisor

import (
	"context"
	"errors"
	"fmt"
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

// TestOnlyCancelled pins which check results say nothing but that the check
// was cancelled: only those whose every leaf is context.Canceled.
func TestOnlyCancelled(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "the cancellation", err: context.Canceled, want: true},
		{name: "wrapped", err: fmt.Errorf("check: %w", context.Canceled), want: true},
		{
			name: "joined cancellations",
			err:  errors.Join(context.Canceled, fmt.Errorf("again: %w", context.Canceled)),
			want: true,
		},
		{name: "a finding", err: errTest, want: false},
		{name: "joined with a finding", err: errors.Join(errTest, context.Canceled), want: false},
		{
			name: "two %w with a finding",
			err:  fmt.Errorf("%w: %w", errTest, context.Canceled),
			want: false,
		},
		{
			name: "a finding deep in the tree",
			err: fmt.Errorf("outer: %w", errors.Join(context.Canceled,
				fmt.Errorf("inner: %w", errTest))),
			want: false,
		},
		{name: "the deadline", err: context.DeadlineExceeded, want: false},
		{name: "an empty join", err: emptyJoinError{}, want: false},
	}

	for _, test := range cases {
		assert.Equal(t, test.want, onlyCancelled(test.err), test.name)
	}
}

// TestStartFuncStartsAndFails pins that a process the StartFunc started but
// did not hand over, because the StartFunc panicked or failed after starting
// it, is killed and waited for, with its group, and does not outlive Run.
func TestStartFuncStartsAndFails(t *testing.T) {
	cases := []struct {
		name   string
		panics bool
		group  bool
	}{
		{name: "panic", panics: true},
		{name: "panic in a group", panics: true, group: true},
		{name: "error", panics: false},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "supervisor.pid")
			spec := helperSpec(t, dir, modeStubborn)

			if test.group {
				spec = helperSpec(t, dir, modeFamily)
				spec.ProcessGroup = true
			}

			var (
				started  atomic.Int64
				cleanups atomic.Int32
			)

			engine, err := New(fixedSource(spec, true), Options{
				PidFile:     pidFile,
				StopSignals: stopSignals,
				Cleanup:     func() { cleanups.Add(1) },
				Start: func(cmd *exec.Cmd) error {
					err := StartCmd(cmd)
					if err != nil {
						return err
					}

					started.Store(int64(cmd.Process.Pid))
					// Only a child that is well under way proves anything.
					waitReady(t, dir, cmd.Process.Pid)

					if test.panics {
						panic(errPanic)
					}

					return errTest
				},
			})
			require.NoError(t, err)

			engine.subscribe = func() (<-chan os.Signal, func()) {
				return make(chan os.Signal), func() {}
			}

			var (
				runErr    error
				recovered any
			)

			func() {
				defer func() { recovered = recover() }()

				runErr = engine.Run(t.Context())
			}()

			pid := int(started.Load())
			require.NotZero(t, pid)
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

			if test.panics {
				assert.Equal(t, errPanic, recovered)
			} else {
				require.ErrorIs(t, runErr, errTest)
				assert.Equal(t, OpStart, errorOp(t, runErr))
			}

			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH,
				"the child outlived Run or was not waited for")

			if test.group {
				grandchild, err := strconv.Atoi(waitFile(t, filepath.Join(dir, "grandchild")))
				require.NoError(t, err)
				t.Cleanup(func() { _ = syscall.Kill(grandchild, syscall.SIGKILL) })
				assert.Eventually(t, func() bool {
					return errors.Is(syscall.Kill(grandchild, 0), syscall.ESRCH)
				}, waitTimeout, pollInterval, "the group outlived Run")
			}

			assert.Equal(t, int32(1), cleanups.Load())
			assert.NoFileExists(t, pidFile)
		})
	}
}

// emptyJoinError is an error joining nothing.
type emptyJoinError struct{}

func (emptyJoinError) Error() string   { return "empty" }
func (emptyJoinError) Unwrap() []error { return nil }

// TestCheckFindingWithCancellation pins that a check that fails as the
// child exits under it, and reports the cancellation along with what it
// found, is a failed check: Run ends with it and the Source, which would
// restart, is not asked.
func TestCheckFindingWithCancellation(t *testing.T) {
	shapes := map[string]func(ctx context.Context) error{
		"joined": func(ctx context.Context) error {
			return errors.Join(errTest, ctx.Err())
		},
		"two %w": func(ctx context.Context) error {
			return fmt.Errorf("%w: %w", errTest, ctx.Err())
		},
	}

	for name, finish := range shapes {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			src := fixedSource(helperSpec(t, dir, modeServe), true)

			var child atomic.Int64

			rec := &recorder{}
			sup := newHarness(t, src, Options{
				StopSignals: stopSignals,
				CheckPeriod: 20 * time.Millisecond,
				Check:       checkWhileChildExits(&child, finish),
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

			_, restarts := src.calls()
			assert.Zero(t, restarts, "the Source was asked about a restart")
		})
	}
}
