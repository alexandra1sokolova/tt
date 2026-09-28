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
// signal the engine receives is handled exactly once, in the order it was
// received. That is not the order, or the number, of the signals sent: the
// kernel merges a signal sent again before the first one was delivered, and
// the Go runtime drops one it cannot buffer. While a child runs:
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
// Spec.ProcessGroup is set.
//
// While no child runs, a signal is handled as there is none to take it: a
// stop signal ends Run, the reload hook runs, and a signal that would have
// been forwarded is dropped. That holds during the restart delay, where
// signals are handled as they arrive, and for the signals that have arrived
// by the moments the engine decides on what to do next: before it asks the
// Source for a Spec, before it starts the child with that Spec, and before it
// asks the Source about a restart. So a stop that has arrived by then always
// wins: no child is started and the Source is not asked anything after it,
// even when the child's exit or the end of the restart delay is ready at the
// same moment. A signal that arrives while the child is being started waits
// in a buffer and is handled, with the child, once it runs. A signal that
// arrives while that buffer is full is lost.
//
// Cancelling the context passed to Run is a stop as well, with
// Spec.StopSignal; it wins over a start or a restart in the same way.
//
// # Checks
//
// With Options.CheckPeriod set, Options.Check runs that often while a child is
// running. When it returns an error, the child (its group, with
// Spec.ProcessGroup) is killed with SIGKILL, and once the child has exited
// Run returns the error without asking about a restart. When the child exits
// while a check runs, the engine cancels the check's context and waits for
// it: an error it returns still ends Run that way, unless it only reports the
// cancellation (it matches context.Canceled).
//
// # Starting
//
// Every child is started by Options.Start, which receives a fully prepared
// exec.Cmd and has to start it. It is the one place to change how a child is
// started, for example to execute bytes that were verified in memory rather
// than whatever the path names by the time of the exec.
//
// Detach starts a process that outlives its caller. While the caller lives,
// it waits for the process in the background, so none is left a zombie.
//
// # Guarantees
//
// When Run returns, the child it started last has exited and has been
// waited for, the checks have stopped, the child's pid file is removed,
// Options.Cleanup has run if the supervisor pid file was taken (or none was
// configured), and the supervisor pid file is removed.
//
// The same holds when a callback panics: the child is killed with SIGKILL
// and waited for, the checks stop, the pid files are removed, Cleanup runs
// (the supervisor pid file goes even if Cleanup panics as well), the signals
// are no longer caught, and then the panic goes on to the caller of Run.
//
// # Limits
//
// These are part of the contract rather than defects:
//
//   - A member of the child's process group that outlives the child is
//     neither killed nor waited for: only the stop timeout and a failed check
//     kill the whole group, and only while the child still runs.
//   - A signal sent to the group after the child has been waited for, in the
//     moment before the engine sees the exit, goes to whatever process group
//     has that id by then; the id can be reused once the whole group is gone.
//   - A Check that ignores the cancellation of its context, or an
//     io.Writer given as Spec.Stdout or Spec.Stderr that blocks, keeps Run
//     from returning until it returns.
//   - Callbacks must not rely on being called after a panic in another
//     callback: only Cleanup is.
package supervisor
