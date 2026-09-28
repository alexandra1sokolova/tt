package cmd

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/tt/sdk/integrity"
	"github.com/tarantool/tt/v3/cli/cmdcontext"
	"github.com/tarantool/tt/v3/cli/process_utils"
)

var errTampered = errors.New("a checked file changed")

// tamperedRepository is an integrity repository whose files have changed.
type tamperedRepository struct{}

func (tamperedRepository) Read(string) (io.ReadCloser, error) { return nil, errTampered }
func (tamperedRepository) ReadFile(string) ([]byte, error)    { return nil, errTampered }
func (tamperedRepository) ValidateAll() error                 { return errTampered }

// tcmShim stands in for TCM: it writes its pid into the file tcm-shim.pid
// and sleeps.
const tcmShim = "#!/bin/sh\necho $$ > tcm-shim.pid\nexec sleep 60\n"

// TestTcmStartRefusedLeavesNoTCM pins that tt tcm start without a watchdog,
// refused because the pid file names a running TCM, leaves no TCM of its own
// running.
func TestTcmStartRefusedLeavesNoTCM(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	running := exec.CommandContext(t.Context(), "sleep", "60")
	require.NoError(t, running.Start())
	t.Cleanup(func() {
		_ = running.Process.Kill()
		_ = running.Wait()
	})

	owned, err := process_utils.CreatePIDFile(tcmPidFile, running.Process.Pid)
	require.NoError(t, err)
	require.NoError(t, owned.Keep())

	shim := filepath.Join(dir, "tcm")
	require.NoError(t, os.WriteFile(shim, []byte(tcmShim), 0o700))

	saved := tcmCtx.Executable

	tcmCtx.Executable = shim

	t.Cleanup(func() { tcmCtx.Executable = saved })

	err = startTcmInteractive("INFO")
	require.ErrorContains(t, err, "the pid file belongs to a running process")

	data, err := os.ReadFile(tcmPidFile)
	require.NoError(t, err)
	assert.Equal(t, strconv.Itoa(running.Process.Pid), string(data))

	// A shim left running writes its pid within the wait; one killed at once
	// may never write it.
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		data, err = os.ReadFile("tcm-shim.pid")
		if err == nil && strings.HasSuffix(string(data), "\n") {
			shimPid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			require.NoError(t, err)
			t.Cleanup(func() { _ = syscall.Kill(shimPid, syscall.SIGKILL) })
			assert.ErrorIs(t, syscall.Kill(shimPid, 0), syscall.ESRCH, "the refused TCM runs")

			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// TestTcmWatchdogOpts pins what the watchdog of TCM runs with: the pid files
// the tcm commands read, a 30 second stop timeout, and the integrity check of
// the environment every --integrity-check-period seconds.
func TestTcmWatchdogOpts(t *testing.T) {
	cmdCtx := &cmdcontext.CmdCtx{}

	cmdCtx.Cli.IntegrityCheckPeriod = 42
	cmdCtx.Integrity = integrity.IntegrityCtx{Repository: tamperedRepository{}}

	opts := tcmWatchdogOpts(cmdCtx, "/opt/tcm")

	assert.Equal(t, "/opt/tcm", opts.Executable)
	assert.Equal(t, "watchdog.pid", opts.PidFile)
	assert.Equal(t, "tcm.pid", opts.ChildPidFile)
	assert.Equal(t, 30*time.Second, opts.StopTimeout)
	assert.Equal(t, 42*time.Second, opts.CheckPeriod)
	require.NotNil(t, opts.Check)
	require.ErrorIs(t, opts.Check(t.Context()), errTampered)
}
