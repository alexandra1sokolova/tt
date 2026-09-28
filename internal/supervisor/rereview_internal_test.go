package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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
