package running

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/tt/sdk/integrity"
	"github.com/tarantool/tt/v3/cli/cmdcontext"
	"github.com/tarantool/tt/v3/cli/ttlog"
	"github.com/tarantool/tt/v3/internal/supervisor"
)

var errTampered = errors.New("a checked file changed")

// tamperedRepository is an integrity repository whose files have changed.
type tamperedRepository struct {
	mockRepository
}

func (*tamperedRepository) ValidateAll() error {
	return errTampered
}

// lockedBuffer is a buffer written by the watchdog and the output of
// tarantool at once, and read by the test meanwhile.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (lb *lockedBuffer) Write(data []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	return lb.buf.Write(data)
}

// Read reads what has been written and not read yet; io.EOF means nothing
// has, for now.
func (lb *lockedBuffer) Read(data []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	return lb.buf.Read(data)
}

func (lb *lockedBuffer) String() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	return lb.buf.String()
}

// watchdogTest is the watchdog of a script instance in a temporary directory,
// with a configuration the test decides.
type watchdogTest struct {
	t       *testing.T
	cmdCtx  *cmdcontext.CmdCtx
	inst    InstanceCtx
	flag    string
	log     *lockedBuffer
	src     *instanceSource
	engine  *supervisor.Engine
	mu      sync.Mutex
	started []int
	// restartable is what the configuration says, read at every exit.
	restartable bool
}

func newWatchdogTest(t *testing.T, restartable bool, cmdCtx *cmdcontext.CmdCtx,
	restartDelay time.Duration,
) *watchdogTest {
	t.Helper()

	appPath, err := filepath.Abs(filepath.Join(instTestAppDir, "dumb_test_app.lua"))
	require.NoError(t, err)

	tarantool, err := exec.LookPath("tarantool")
	require.NoError(t, err)

	cmdCtx.Cli.TarantoolCli.Executable = tarantool

	dir := t.TempDir()
	test := &watchdogTest{
		t:      t,
		cmdCtx: cmdCtx,
		inst: InstanceCtx{
			AppDir:         dir,
			InstanceScript: appPath,
			WalDir:         dir,
			VinylDir:       dir,
			MemtxDir:       dir,
			PIDFile:        filepath.Join(dir, "tt.pid"),
			Restartable:    restartable,
		},
		flag:        filepath.Join(dir, "started"),
		log:         &lockedBuffer{},
		restartable: restartable,
	}

	t.Setenv("started_flag_file", test.flag)

	src := &instanceSource{
		cmdCtx: cmdCtx,
		inst:   test.inst,
		log: &watchdogLog{
			logger:      ttlog.NewCustomLogger(test.log, "Watchdog ", 0),
			checkPeriod: time.Duration(cmdCtx.Cli.IntegrityCheckPeriod) * time.Second,
		},
		refresh: func() (InstanceCtx, error) {
			test.mu.Lock()
			defer test.mu.Unlock()

			inst := test.inst

			inst.Restartable = test.restartable

			return inst, nil
		},
	}

	test.src = src

	opts := watchdogOptions(cmdCtx, src)
	onEvent := opts.OnEvent

	opts.RestartDelay = restartDelay
	opts.OnEvent = func(event supervisor.Event) {
		started, ok := event.(supervisor.Started)
		if ok {
			test.recordStart(started.Pid)
		}

		onEvent(event)
	}

	test.engine, err = supervisor.New(src, opts)
	require.NoError(t, err)

	return test
}

// run runs the watchdog, logging its end as Start does, and returns the
// channel Run's result comes on.
func (test *watchdogTest) run() <-chan error {
	done := make(chan error, 1)

	go func() {
		err := test.engine.Run(context.Background())
		test.src.log.onEnd(err)

		done <- err
	}()

	test.t.Cleanup(func() {
		for _, pid := range test.children() {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	return done
}

func (test *watchdogTest) recordStart(pid int) {
	test.mu.Lock()
	defer test.mu.Unlock()

	test.started = append(test.started, pid)
}

func (test *watchdogTest) children() []int {
	test.mu.Lock()
	defer test.mu.Unlock()

	return append([]int(nil), test.started...)
}

// waitStarted waits for the nth start of tarantool and returns its pid.
func (test *watchdogTest) waitStarted(nth int) int {
	test.t.Helper()

	require.Eventually(test.t, func() bool {
		_, err := os.Stat(test.flag)

		return err == nil && len(test.children()) >= nth
	}, 20*time.Second, 10*time.Millisecond, "tarantool did not start")

	require.NoError(test.t, os.Remove(test.flag))

	return test.children()[nth-1]
}

// waitDone waits for Run to return.
func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(40 * time.Second):
		require.FailNow(t, "the watchdog did not end")

		return nil
	}
}

// TestWatchdogRestartsAndStops pins the watchdog of a restartable instance:
// tarantool that dies, by SIGINT or SIGKILL, starts again, and a SIGINT to
// the watchdog stops it and the watchdog, which then removes its pid file.
func TestWatchdogRestartsAndStops(t *testing.T) {
	test := newWatchdogTest(t, true, &cmdcontext.CmdCtx{}, 100*time.Millisecond)
	done := test.run()

	first := test.waitStarted(1)
	assert.Equal(t, os.Getpid(), readPidFile(t, test.inst.PIDFile),
		"the pid file names the watchdog")

	require.NoError(t, syscall.Kill(first, syscall.SIGINT))

	second := test.waitStarted(2)
	require.NoError(t, syscall.Kill(second, syscall.SIGKILL))
	test.waitStarted(3)

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))
	require.NoError(t, waitDone(t, done))

	assert.NoFileExists(t, test.inst.PIDFile)
	assert.Contains(t, test.log.String(), "(INFO): interrupt received.")
	assert.Contains(t, test.log.String(), "(INFO): waiting for restart timeout 100ms.")
	assert.Contains(t, test.log.String(), "(INFO): the Instance has shutdown.")
}

// TestWatchdogNotRestartable pins that tarantool that dies is not started
// again when the configuration does not say restart_on_failure.
func TestWatchdogNotRestartable(t *testing.T) {
	test := newWatchdogTest(t, false, &cmdcontext.CmdCtx{}, 100*time.Millisecond)
	done := test.run()

	first := test.waitStarted(1)
	require.NoError(t, syscall.Kill(first, syscall.SIGKILL))
	require.NoError(t, waitDone(t, done))

	assert.Len(t, test.children(), 1)
	assert.NoFileExists(t, test.inst.PIDFile)
	assert.Contains(t, test.log.String(), "(INFO): the Instance has shutdown.")
}

// TestWatchdogIntegrityHardStop pins that a failed integrity check stops the
// instance for good, although the configuration says restart_on_failure.
func TestWatchdogIntegrityHardStop(t *testing.T) {
	cmdCtx := &cmdcontext.CmdCtx{}

	cmdCtx.Cli.IntegrityCheckPeriod = 1
	cmdCtx.Integrity = integrity.IntegrityCtx{Repository: &tamperedRepository{}}

	test := newWatchdogTest(t, true, cmdCtx, 100*time.Millisecond)
	done := test.run()

	test.waitStarted(1)

	err := waitDone(t, done)
	require.ErrorIs(t, err, errTampered)

	var runErr *supervisor.Error

	require.ErrorAs(t, err, &runErr)
	assert.Equal(t, supervisor.OpCheck, runErr.Op)
	assert.Len(t, test.children(), 1, "tarantool started again after a failed check")
	assert.Contains(t, test.log.String(), "(ERROR): periodic integrity check failed:")
	assert.Contains(t, test.log.String(), "is not restarted")
	assert.NoFileExists(t, test.inst.PIDFile)
}

// readPidFile reads the pid in a pid file.
func readPidFile(t *testing.T, path string) int {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	pid, err := strconv.Atoi(string(data))
	require.NoError(t, err)

	return pid
}
