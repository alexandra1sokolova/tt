// Package supervisor runs a child process and keeps it running.
//
// An Engine asks its Source for a Spec before every start, including the
// first, starts the child from that Spec and supervises it until it exits.
// After an exit the Source decides whether the child runs again; a restart
// waits Options.RestartDelay and then asks the Source for a fresh Spec, so a
// consumer that builds the Spec from its configuration re-reads it on every
// start.
//
// # Serialization
//
// Everything that changes the state of the supervision happens on the
// goroutine that called Engine.Run: starting the child, handling signals,
// stopping and killing the child, deciding on and waiting for a restart,
// writing and removing pid files. Every callback except Options.Check is
// called from that goroutine too (Source.Next, Source.Restart,
// Options.OnEvent, Options.OnReload, Options.Cleanup), so the callbacks never
// run concurrently with each other. Three helper goroutines feed the loop and
// never act on their own: one relays OS signals, one waits for the child and
// one runs the periodic check while a child is running.
//
// # Pid files
//
// Options.PidFile is the supervisor's own pid file. Run creates it after it
// has subscribed to signals, so whoever finds the pid of the supervisor there
// can already stop it with a signal. It is created with O_EXCL and refused
// while it names a live process; a file naming a dead one is replaced. If it
// cannot be created, Run returns before starting anything and does not run
// Options.Cleanup: whatever is out there belongs to someone else. Once
// created, it is removed when Run returns, after the child is gone and
// Options.Cleanup has run, so its disappearance means the supervision is
// over.
//
// Options.ChildPidFile is the child's pid file. It is written after every
// start, before the Started event, and removed after every exit. If it cannot
// be written, the child is killed and Run returns.
//
// A pid file is removed only while it still names the pid written into it.
//
// # Signals
//
// Run catches every signal but SIGURG, which the Go runtime sends itself for
// preemption, and SIGCHLD, which reports the supervisor's own children. Each
// caught signal is handled exactly once, in arrival order:
//
//   - A stop signal (Options.StopSignals) stops the child: it is sent
//     Spec.StopSignal, or the received signal itself when
//     Spec.ForwardStopSignal is set. A child still alive Spec.StopTimeout
//     after the first stop signal is killed with SIGKILL. Once the child has
//     exited, Run returns without asking about a restart. A further stop
//     signal during the stop is sent to the child as well, but does not
//     restart the timeout.
//   - The reload signal (Options.ReloadSignal) runs Options.OnReload and is
//     then forwarded to the child.
//   - Any other signal is forwarded to the child.
//
// Signals go to the child, or to its whole process group when
// Spec.ProcessGroup is set. A signal that arrives while the child is being
// started waits in a buffer and is handled once the child runs. A signal that
// arrives during the restart delay is handled at once: a stop signal ends Run
// without a restart, the reload hook runs, and a signal that would have been
// forwarded is dropped, because there is no child to take it. A signal that
// arrives while the buffer is full is lost.
//
// Cancelling the context passed to Run is a stop as well, with
// Spec.StopSignal.
//
// # Checks
//
// With Options.CheckPeriod set, Options.Check runs that often while a child is
// running. When it returns an error, the child (its group, with
// Spec.ProcessGroup) is killed with SIGKILL, and once the child has exited
// Run returns the error without asking about a restart.
//
// # Starting
//
// Every child is started by Options.Start, which receives a fully prepared
// exec.Cmd and has to start it. It is the one place to change how a child is
// started, for example to execute bytes that were verified in memory rather
// than whatever the path names by the time of the exec.
//
// # Guarantees
//
// When Run returns, the child it started last has exited and has been
// waited for, its pid file is removed, Options.Cleanup has run if the
// supervisor pid file was created (or none was configured), and the
// supervisor pid file is removed. A member of the child's process group that
// outlived the child is not waited for: only the stop timeout and a failed
// check kill the whole group.
package supervisor
