package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
)

// ErrNotStarted is returned when a StartFunc reports success without having
// started the process.
var ErrNotStarted = errors.New("the start function returned without a process")

// StartFunc starts a fully prepared command, the way exec.Cmd.Start does:
// on success cmd.Process is the running process, and the caller waits for it
// with cmd.Wait. It may change how the process is started, as long as the
// process started is a child of the caller.
type StartFunc func(cmd *exec.Cmd) error

// StartCmd is the StartFunc that calls cmd.Start.
func StartCmd(cmd *exec.Cmd) error {
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("starting %q: %w", cmd.Path, err)
	}

	return nil
}

// child is a started child process.
type child struct {
	cmd *exec.Cmd
	pid int
	// group tells that the child leads a process group of its own and signals
	// go to the group.
	group bool
	// done receives the exit of the child exactly once.
	done chan Exit
	// pidFile is the child's pid file while the engine owns one.
	pidFile *pidFile
}

// command prepares the command for spec.
func command(ctx context.Context, spec *Spec) *exec.Cmd {
	// The engine stops the child itself; the context must not kill it.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), spec.Path, spec.Args...)

	cmd.Env = spec.Env
	cmd.Dir = spec.Dir

	if spec.Stdin != nil {
		cmd.Stdin = bytes.NewReader(spec.Stdin)
	}

	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: spec.ProcessGroup}
	cmd.WaitDelay = spec.StopTimeout

	return cmd
}

// startChild starts the child for spec through start and begins waiting for
// it.
func startChild(ctx context.Context, spec *Spec, start StartFunc) (*child, error) {
	cmd := command(ctx, spec)

	err := start(cmd)
	if err != nil {
		return nil, err
	}

	if cmd.Process == nil {
		return nil, ErrNotStarted
	}

	proc := &child{
		cmd:     cmd,
		pid:     cmd.Process.Pid,
		group:   spec.ProcessGroup,
		done:    make(chan Exit, 1),
		pidFile: nil,
	}

	go func() {
		err := cmd.Wait()
		proc.done <- Exit{Pid: proc.pid, State: cmd.ProcessState, Err: err}
	}()

	return proc, nil
}

// signal sends sig to the child, or to its process group.
func (proc *child) signal(sig syscall.Signal) error {
	if proc.group {
		err := syscall.Kill(-proc.pid, sig)
		if err != nil {
			return fmt.Errorf("sending %s to the process group %d: %w", sig, proc.pid, err)
		}

		return nil
	}

	err := proc.cmd.Process.Signal(sig)
	if err != nil {
		return fmt.Errorf("sending %s to the process %d: %w", sig, proc.pid, err)
	}

	return nil
}
