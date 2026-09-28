package supervisor

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/tarantool/tt/v3/cli/process_utils"
)

// createPidFile writes pid into path, refusing a file that names a live
// process and replacing one that names a dead process.
func createPidFile(path string, pid int) error {
	return process_utils.CreatePIDFile(path, pid)
}

// removePidFile removes path if it still names pid. A missing file, or one
// that names another process, is left as it is.
func removePidFile(path string, pid int) error {
	got, err := process_utils.GetPIDFromFile(path)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return err
	case got != pid:
		return nil
	}

	err = os.Remove(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing the pid file: %w", err)
	}

	return nil
}
