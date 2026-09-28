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
	exits   []supervisor.Exit
	// restartable is what the configuration says, read at every exit.
	restartable bool
}

// testRestartDelay is the restart delay of the watchdog of a test.
const testRestartDelay = 100 * time.Millisecond

func newWatchdogTest(t *testing.T, restartable bool, cmdCtx *cmdcontext.CmdCtx) *watchdogTest {
	t.Helper()

	appPath, err := filepath.Abs(filepath.Join(instTestAppDir, "dumb_test_app.lua"))
	require.NoError(t, err)

	tarantool, err := exec.LookPath("tarantool")
	require.NoError(t, err)

	cmdCtx.Cli.TarantoolCli.Executable = tarantool

	// The sockets go in a directory of their own, short enough for a unix
	// socket path.
	dir := t.TempDir()
	runDir := shortTempDir(t)
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
			ConsoleSocket:  filepath.Join(runDir, "tarantool.control"),
			BinaryPort:     filepath.Join(runDir, "tarantool.sock"),
			Restartable:    restartable,
		},
		flag:        filepath.Join(dir, "started"),
		log:         &lockedBuffer{},
		restartable: restartable,
	}

	t.Setenv("started_flag_file", test.flag)

	test.src = newInstanceSource(cmdCtx, &test.inst, &watchdogLog{
		logger:      ttlog.NewCustomLogger(test.log, "Watchdog ", 0),
		checkPeriod: time.Duration(cmdCtx.Cli.IntegrityCheckPeriod) * time.Second,
	}, func() (InstanceCtx, error) {
		test.mu.Lock()
		defer test.mu.Unlock()

		inst := test.inst

		inst.Restartable = test.restartable

		return inst, nil
	})

	opts := watchdogOptions(cmdCtx, test.src)
	onEvent := opts.OnEvent

	opts.RestartDelay = testRestartDelay
	opts.OnEvent = func(event supervisor.Event) {
		switch event := event.(type) {
		case supervisor.Started:
			test.recordStart(event.Pid)
		case supervisor.Exit:
			test.recordExit(event)
		}

		onEvent(event)
	}

	test.engine, err = supervisor.New(test.src, opts)
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

func (test *watchdogTest) recordExit(exit supervisor.Exit) {
	test.mu.Lock()
	defer test.mu.Unlock()

	test.exits = append(test.exits, exit)
}

func (test *watchdogTest) exitsSoFar() []supervisor.Exit {
	test.mu.Lock()
	defer test.mu.Unlock()

	return append([]supervisor.Exit(nil), test.exits...)
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
	test := newWatchdogTest(t, true, &cmdcontext.CmdCtx{})
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
	test := newWatchdogTest(t, false, &cmdcontext.CmdCtx{})
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

	test := newWatchdogTest(t, true, cmdCtx)
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

// shortTempDir returns a directory under /tmp, whose path leaves room for a
// unix socket in it.
func shortTempDir(t *testing.T) string {
	t.Helper()

	//nolint:usetesting // t.TempDir is too deep for a socket path on macOS.
	dir, err := os.MkdirTemp("/tmp", "ttw")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return dir
}

// moveRunDir changes where the configuration puts the sockets, as an edit of
// run_dir would, and returns the new context.
func (test *watchdogTest) moveRunDir() InstanceCtx {
	test.t.Helper()

	runDir := shortTempDir(test.t)

	test.mu.Lock()
	defer test.mu.Unlock()

	test.inst.ConsoleSocket = filepath.Join(runDir, "tarantool.control")
	test.inst.BinaryPort = filepath.Join(runDir, "tarantool.sock")

	return test.inst
}

// setRestartable changes what the configuration says of restart_on_failure.
func (test *watchdogTest) setRestartable(restartable bool) {
	test.mu.Lock()
	defer test.mu.Unlock()

	test.restartable = restartable
}

// waitSockets waits for the sockets of inst to exist.
func waitSockets(t *testing.T, inst *InstanceCtx) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, consoleErr := os.Stat(inst.ConsoleSocket)
		_, binaryErr := os.Stat(inst.BinaryPort)

		return consoleErr == nil && binaryErr == nil
	}, 20*time.Second, 10*time.Millisecond, "tarantool did not create its sockets")
}

// assertNoSockets asserts that the sockets of inst are gone.
func assertNoSockets(t *testing.T, inst *InstanceCtx) {
	t.Helper()

	assert.NoFileExists(t, inst.ConsoleSocket)
	assert.NoFileExists(t, inst.BinaryPort)
}

// TestWatchdogCleanupAfterHardStop pins that the watchdog removes the sockets
// tarantool leaves when it is killed, here by a failed integrity check.
func TestWatchdogCleanupAfterHardStop(t *testing.T) {
	cmdCtx := &cmdcontext.CmdCtx{}

	cmdCtx.Cli.IntegrityCheckPeriod = 1
	cmdCtx.Integrity = integrity.IntegrityCtx{Repository: &tamperedRepository{}}

	test := newWatchdogTest(t, true, cmdCtx)
	done := test.run()

	test.waitStarted(1)
	waitSockets(t, &test.inst)

	require.ErrorIs(t, waitDone(t, done), errTampered)
	assertNoSockets(t, &test.inst)
}

// TestWatchdogCleanupAfterRunDirChange pins that the sockets removed after
// tarantool exits are the ones it ran with, when the configuration has moved
// them meanwhile: at the end of the watchdog when the instance is not
// restarted, and before the next start when it is.
func TestWatchdogCleanupAfterRunDirChange(t *testing.T) {
	for _, restartable := range []bool{false, true} {
		t.Run("restartable="+strconv.FormatBool(restartable), func(t *testing.T) {
			test := newWatchdogTest(t, true, &cmdcontext.CmdCtx{})
			done := test.run()

			first := test.waitStarted(1)
			old := test.inst

			waitSockets(t, &old)

			moved := test.moveRunDir()

			test.setRestartable(restartable)

			// Killed, tarantool leaves its sockets behind.
			require.NoError(t, syscall.Kill(first, syscall.SIGKILL))

			if restartable {
				test.waitStarted(2)
				waitSockets(t, &moved)
				assertNoSockets(t, &old)

				require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))
			}

			require.NoError(t, waitDone(t, done))
			assertNoSockets(t, &old)
			assertNoSockets(t, &moved)
		})
	}
}

// TestWatchdogForwardsStopSignal pins that a stop signal reaches tarantool
// as it was sent: SIGQUIT stays SIGQUIT, which tt quit relies on.
func TestWatchdogForwardsStopSignal(t *testing.T) {
	// Without a core file for the SIGQUIT tarantool dies of.
	var limit syscall.Rlimit

	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_CORE, &limit))

	noCore := syscall.Rlimit{Cur: 0, Max: limit.Max}

	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_CORE, &noCore))
	t.Cleanup(func() { _ = syscall.Setrlimit(syscall.RLIMIT_CORE, &limit) })

	test := newWatchdogTest(t, true, &cmdcontext.CmdCtx{})
	done := test.run()

	test.waitStarted(1)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGQUIT))
	require.NoError(t, waitDone(t, done))

	exits := test.exitsSoFar()
	require.Len(t, exits, 1)
	require.NotNil(t, exits[0].State)

	status, ok := exits[0].State.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	assert.True(t, status.Signaled(), "tarantool exited with %v", exits[0].State)
	assert.Equal(t, syscall.SIGQUIT, status.Signal())
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
