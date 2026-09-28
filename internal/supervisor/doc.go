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
// A pid file holds the decimal pid without a newline, the format tt stop and
// tt status read. Owning one means holding an exclusive flock on it, taken
// without waiting and kept for as long as the pid in it is current; the lock
// descriptor is close-on-exec, so no child inherits it, and the kernel drops
// it when the owner dies, however it dies. A file whose lock is held is
// refused. A file nobody holds is stale and is taken over in place, under the
// lock, unless it names a live process: such a file comes from a tt that
// does not lock pid files, and that process still runs. Of any number of
// processes racing for one pid file, exactly one owns it. An owner unlinks
// the file before it unlocks it, and a process that locked a file no longer
// at the path starts over, so ownership never passes to an unlinked file.
// Only the file the owner locked is removed, never one that replaced it.
//
// Options.PidFile is the supervisor's own pid file. Run takes it after it has
// subscribed to signals, so whoever finds the pid of the supervisor there can
// already stop it with a signal. If Run cannot take it, it returns before
// starting anything and does not run Options.Cleanup: whatever is out there
// belongs to someone else. Once taken, it is removed when Run returns, after
// the child is gone and Options.Cleanup has run, so its disappearance means
// the supervision is over.
//
// Options.ChildPidFile is the child's pid file. It is written after every
// start, before the Started event, and removed after every exit. If it cannot
// be written, the child is killed and Run returns.
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
