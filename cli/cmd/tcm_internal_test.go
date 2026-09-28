package cmd

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/tt/sdk/integrity"
	"github.com/tarantool/tt/v3/cli/cmdcontext"
)

var errTampered = errors.New("a checked file changed")

// tamperedRepository is an integrity repository whose files have changed.
type tamperedRepository struct{}

func (tamperedRepository) Read(string) (io.ReadCloser, error) { return nil, errTampered }
func (tamperedRepository) ReadFile(string) ([]byte, error)    { return nil, errTampered }
func (tamperedRepository) ValidateAll() error                 { return errTampered }

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
