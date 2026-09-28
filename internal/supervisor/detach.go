package supervisor

import (
	"context"
	"fmt"
	"os/exec"
	"syscall"
)

// Detach starts path with args in a process group of its own and does not
// wait for it: the process keeps running after the caller exits and is not
// stopped by signals sent to the caller's group. Its standard streams are
// connected to the null device and it inherits the caller's environment.
// start starts the process; nil means StartCmd. Detach returns the pid.
func Detach(path string, args []string, start StartFunc) (int, error) {
	if start == nil {
		start = StartCmd
	}

	cmd := exec.CommandContext(context.Background(), path, args...)

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	err := start(cmd)
	if err != nil {
		return 0, err
	}

	if cmd.Process == nil {
		return 0, ErrNotStarted
	}

	pid := cmd.Process.Pid

	err = cmd.Process.Release()
	if err != nil {
		return 0, fmt.Errorf("releasing the process %d: %w", pid, err)
	}

	return pid, nil
}
