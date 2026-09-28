package supervisor

import (
	"context"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckedAfterTheCheck pins that the Checked event of a passing check
// comes only once the check has returned, so a consumer never reports a
// check as passed before it ran.
func TestCheckedAfterTheCheck(t *testing.T) {
	dir := t.TempDir()

	var (
		lock     sync.Mutex
		finished []time.Time
	)

	rec := &recorder{}
	sup := newHarness(t, fixedSource(helperSpec(t, dir, modeServe), false), Options{
		StopSignals: stopSignals,
		CheckPeriod: 10 * time.Millisecond,
		Check: func(context.Context) error {
			time.Sleep(20 * time.Millisecond)
			lock.Lock()

			finished = append(finished, time.Now())

			lock.Unlock()

			return nil
		},
		OnEvent: rec.record,
	})

	sup.rec = rec
	sup.start()

	started := waitEvent[Started](t, rec, 1)
	waitEvent[Checked](t, rec, 3)
	waitReady(t, dir, started.Pid)
	sup.send(syscall.SIGTERM)
	require.NoError(t, sup.wait())

	_, reported := eventsOf[Checked](rec)

	lock.Lock()
	defer lock.Unlock()

	require.GreaterOrEqual(t, len(finished), len(reported))

	for index, at := range reported {
		assert.False(t, at.Before(finished[index]), "check %d reported before it ended", index)
	}
}

// TestIgnoreSignals pins the ignore set: an ignored signal is dropped with
// an event, never forwarded and never stopping anything, with a child and
// without one.
func TestIgnoreSignals(t *testing.T) {
	dir := t.TempDir()
	src := fixedSource(helperSpec(t, dir, modeServe), true)

	src.restart = func(n int, _ Exit) (bool, error) { return n < 2, nil }

	sup := newHarness(t, src, Options{
		StopSignals:   stopSignals,
		IgnoreSignals: []syscall.Signal{syscall.SIGUSR2, syscall.SIGHUP},
		RestartDelay:  time.Hour,
	})
	sup.start()

	first := waitEvent[Started](t, sup.rec, 1)
	waitReady(t, dir, first.Pid)

	// The ignored signals go first; a forwarded one after them marks when
	// they would have arrived.
	sup.send(syscall.SIGUSR2, syscall.SIGHUP, syscall.SIGUSR1)
	waitSignals(t, dir, first.Pid, syscall.SIGUSR1)

	recorded, err := os.ReadFile(helperFile(dir, "signals", first.Pid))
	require.NoError(t, err)
	assert.Equal(t, syscall.SIGUSR1.String(), strings.TrimSpace(string(recorded)))

	// Without a child: the child exits, the restart delay runs.
	require.NoError(t, syscall.Kill(first.Pid, syscall.SIGKILL))
	waitEvent[Restarting](t, sup.rec, 1)
	sup.send(syscall.SIGHUP, syscall.SIGUSR2, syscall.SIGTERM)
	require.NoError(t, sup.wait())

	got, _ := eventsOf[SignalReceived](sup.rec)
	assert.Equal(t, []SignalReceived{
		{Signal: syscall.SIGUSR2, Action: ActionIgnore},
		{Signal: syscall.SIGHUP, Action: ActionIgnore},
		{Signal: syscall.SIGUSR1, Action: ActionForward},
		{Signal: syscall.SIGHUP, Action: ActionIgnore},
		{Signal: syscall.SIGUSR2, Action: ActionIgnore},
		{Signal: syscall.SIGTERM, Action: ActionStop},
	}, got)
}
