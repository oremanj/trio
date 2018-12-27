import math
import os
import select
import subprocess
import sys
import attr
from async_generator import async_generator, yield_, asynccontextmanager

from . import _core
from ._abc import AsyncResource, HalfCloseableStream, ReceiveStream
from ._sync import CapacityLimiter, Lock
from ._threads import run_sync_in_worker_thread
from ._highlevel_generic import NullStream
from ._subprocess_platform import (
    wait_child_exiting, create_pipe_to_child_stdin,
    create_pipe_from_child_output
)

__all__ = ["Process"]


class Process(AsyncResource):
    """Execute a child program in a new process.

    Like :class:`subprocess.Popen`, but async.

    Constructing a :class:`Process` immediately spawns the child
    process, or throws an :exc:`OSError` if the spawning fails (for
    example, if the specified command could not be found).
    After construction, you can communicate with the child process
    by writing data to its :attr:`stdin` stream (a
    :class:`~trio.abc.SendStream`), reading data from its :attr:`stdout`
    and/or :attr:`stderr` streams (both :class:`~trio.abc.ReceiveStream`\s),
    sending it signals using :meth:`terminate`, :meth:`kill`, or
    :meth:`send_signal`, and waiting for it to exit using :meth:`wait`.

    Each standard stream is only available if it was specified at
    :class:`Process` construction time that a pipe should be created
    for it.  For example, if you constructed with
    ``stdin=subprocess.PIPE``, you can write to the :attr:`stdin`
    stream, else :attr:`stdin` will be ``None``.

    :class:`Process` implements :class:`~trio.abc.AsyncResource`, so
    you can use it as an async context manager or call its
    :meth:`aclose` method directly. "Closing" a :class:`Process` will
    close any pipes to the child and wait for it to exit; if the call
    to :meth:`aclose` is cancelled, the child will be forcibly killed
    and we will ensure it has finished exiting before allowing the
    cancellation to propagate. It is *strongly recommended* that
    process lifetime be scoped using an ``async with`` block wherever
    possible, to avoid winding up with processes hanging around longer
    than you were planning on.

    Args:
      command (str or list): The command to run. Typically this is a
          sequence of strings such as ``['ls', '-l', 'directory with spaces']``,
          where the first element names the executable to invoke and the other
          elements specify its arguments. It may also be a single string
          with space-separated words, in which case :ref:`quoting rules
          <subprocess-quoting>` come into play.
      stdin: Specifies what the child process's standard input
          stream should connect to: output written by the parent
          (``subprocess.PIPE``), nothing (``subprocess.DEVNULL``),
          or an open file (pass a file descriptor or something whose
          ``fileno`` method returns one). If ``stdin`` is unspecified,
          the child process will have the same standard input stream
          as its parent.
      stdout: Like ``stdin``, but for the child process's standard output
          stream.
      stderr: Like ``stdin``, but for the child process's standard error
          stream. An additional value ``subprocess.STDOUT`` is supported,
          which causes the child's standard output and standard error
          messages to be intermixed on a single standard output stream,
          attached to whatever the ``stdout`` option says to attach it to.
      local_quotes (bool): If true, assume a string ``command`` is quoted
          following the conventions of the platform we're running on.
          If false (the default), assume it is quoted following POSIX
          conventions, and convert to Windows conventions if we're running
          on Windows.
      **options: Other :ref:`general subprocess options <subprocess-options>`
          are also accepted.

    Attributes:
      args (str or list): The ``command`` passed at construction time,
          speifying the process to execute and its arguments.
      pid (int): The process ID of the child process managed by this object.
      stdin (trio.abc.SendStream or None): A stream connected to the child's
          standard input stream: when you write bytes here, they become available
          for the child to read. Only available if the :class:`Process`
          was constructed using ``stdin=PIPE``; otherwise this will be None.
      stdout (trio.abc.ReceiveStream or None): A stream connected to
          the child's standard output stream: when the child writes to
          standard output, the written bytes become available for you
          to read here. Only available if the :class:`Process` was
          constructed using ``stdout=PIPE``; otherwise this will be None.
      stderr (trio.abc.ReceiveStream or None): A stream connected to
          the child's standard error stream: when the child writes to
          standard error, the written bytes become available for you
          to read here. Only available if the :class:`Process` was
          constructed using ``stderr=PIPE``; otherwise this will be None.

    """

    universal_newlines = False
    encoding = None
    errors = None

    # Available for the per-platform wait_child_exiting() implementations
    # to stash some state; waitid platforms use this to avoid spawning
    # arbitrarily many threads if wait() keeps getting cancelled.
    _wait_for_exit_data = None

    def __init__(
        self, command, *, stdin=None, stdout=None, stderr=None, **options
    ):
        for key in (
            'universal_newlines', 'text', 'encoding', 'errors', 'bufsize'
        ):
            if options.get(key):
                raise TypeError(
                    "trio.Process only supports communicating over "
                    "unbuffered byte streams; the '{}' option is not supported"
                    .format(key)
                )

        self.stdin = None
        self.stdout = None
        self.stderr = None

        # State needed by the logic in the property `failed`:
        # - signals we've sent the process before we knew it had exited
        self._signals_sent_before_exit = set()

        # - whether we closed its stdout or stderr without receiving EOF
        #   before we knew it had exited
        self._maybe_caused_broken_pipe_before_exit = None

        # Get arguments into the right form for the underlying API
        local_quotes = options.pop("local_quotes", False)
        if os.name == "posix":
            if options.get("shell"):
                # Wants a POSIX-quoted string
                if not isinstance(command, str):
                    command = " ".join(shlex.quote(arg) for arg in command)
            else:
                # Wants a list
                if isinstance(command, str):
                    command = shlex.split(command)
        else:
            # Wants a list or a Windows-quoted string, regardless of
            # whether shell is True or False
            if isinstance(command, str) and not local_quotes:
                command = shlex.split(command)

        if stdin == subprocess.PIPE:
            self.stdin, stdin = create_pipe_to_child_stdin()
        if stdout == subprocess.PIPE:
            self.stdout, stdout = create_pipe_from_child_output()
        if stderr == subprocess.STDOUT:
            # If we created a pipe for stdout, pass the same pipe for
            # stderr.  If stdout was some non-pipe thing (DEVNULL or a
            # given FD), pass the same thing. If stdout was passed as
            # None, keep stderr as STDOUT to allow subprocess to dup
            # our stdout. Regardless of which of these is applicable,
            # don't create a new trio stream for stderr -- if stdout
            # is piped, stderr will be intermixed on the stdout stream.
            if stdout is not None:
                stderr = stdout
        elif stderr == subprocess.PIPE:
            self.stderr, stderr = create_pipe_from_child_output()

        try:
            self._proc = subprocess.Popen(
                command, stdin=stdin, stdout=stdout, stderr=stderr, **options
            )
        finally:
            # Close the parent's handle for each child side of a pipe;
            # we want the child to have the only copy, so that when
            # it exits we can read EOF on our side.
            if self.stdin is not None:
                os.close(stdin)
            if self.stdout is not None:
                os.close(stdout)
            if self.stderr is not None:
                os.close(stderr)

        self.args = self._proc.args
        self.pid = self._proc.pid

    def __repr__(self):
        if self.returncode is None:
            status = "running with PID {}".format(self.pid)
        else:
            if self.returncode < 0:
                status = "exited with signal {}".format(-self.returncode)
            else:
                status = "exited with status {}".format(self.returncode)
            if self.returncode != 0 and not self.failed:
                status += " (our fault)"
        return "<trio.Process {!r}: {}>".format(self.args, self.status)

    @property
    def returncode(self):
        """The exit status of the process (an integer), or ``None`` if it is
        not yet known to have exited.

        By convention, a return code of zero indicates success.  On
        UNIX, negative values indicate termination due to a signal,
        e.g., -11 if terminated by signal 11 (``SIGSEGV``).  On
        Windows, a process that exits due to a call to
        :meth:`Process.terminate` will have an exit status of 1.

        Accessing this attribute does not check for termination; use
        :meth:`poll` or :meth:`wait` for that.
        """
        return self._proc.returncode

    @property
    def failed(self):
        """Whether this process should be considered to have failed.

        If the process hasn't exited yet, this is None. Otherwise, it is False
        if the :attr:`returncode` is zero, and True otherwise, unless the
        nonzero exit status appears to have been potentially caused by
        something done by the parent Trio process. Specifically:

        * If we terminate a process and it later exits for any reason,
          we don't consider that a failure. (On Windows it's hard to
          tell if the exit was caused by the termination or not, but
          termination can't be blocked so it's a good bet. On UNIX
          there are plenty of processes that respond to ``SIGTERM`` by
          cleaning up and then exiting with a nonzero status, and we'd
          like to consider that to be caused by the ``SIGTERM`` even
          though it's not reported as "exited due to ``SIGTERM``"
          directly.)

        * If we close a process's standard output or error stream
          before receiving EOF on it and the process later exits for
          any reason, we don't consider that a failure. (We could
          restrict this to only ``SIGPIPE`` exits on UNIX, but then we
          wouldn't handle processes that catch ``SIGPIPE`` and raise
          an ordinary error instead, like Python.)

        * On UNIX, if we send a process a signal and it later exits due
          to that signal, we don't consider that a failure. (This covers
          signals like ``SIGHUP``, ``SIGINT``, etc, which are exit
          signals for some processes and not for others.)

        """
        if self.returncode is None:
            return None
        return (
            self.returncode != 0
            and signal.SIGTERM not in self._signals_sent_before_exit
            and not self._maybe_caused_broken_pipe_before_exit
            and -self.returncode not in self._signals_sent_before_exit
        )

    async def aclose(self):
        """Close any pipes we have to the process (both input and output)
        and wait for it to exit.

        If cancelled, kills the process and waits for it to finish
        exiting before propagating the cancellation.
        """
        with _core.open_cancel_scope(shield=True):
            if self.stdin is not None:
                await self.stdin.aclose()
            if self.stdout is not None:
                await self.stdout.aclose()
            if self.stderr is not None:
                await self.stderr.aclose()
        try:
            await self.wait()
        finally:
            if self.returncode is None:
                self.kill()
                with _core.open_cancel_scope(shield=True):
                    await self.wait()

    def _check_maybe_caused_broken_pipe(self):
        """Called when the child process exits to set
        self._maybe_caused_broken_pipe_before_exit, based on whether
        we have yet closed the child's stdout or stderr before reading
        EOF from it. This is an input to the logic in ``failed``.
        """
        if self._maybe_caused_broken_pipe_before_exit is not None:
            return
        for stream in (self.stdout, self.stderr):
            if stream is not None and stream.did_close_before_receiving_eof:
                self._maybe_caused_broken_pipe_before_exit = True
                break
        else:
            self._maybe_caused_broken_pipe_before_exit = False

    async def wait(self):
        """Block until the process exits.

        Returns:
          The exit status of the process; see :attr:`returncode`.
        """
        if self.poll() is None:
            await wait_child_exiting(self)
            self._proc.wait()
            self._check_maybe_caused_broken_pipe()
        else:
            await _core.checkpoint()
        return self.returncode

    def poll(self):
        """Check if the process has exited yet.

        Returns:
          The exit status of the process; see :attr:`returncode`.
        """
        if self.returncode is None and self._proc.poll() is not None:
            self._check_maybe_caused_broken_pipe()
        return self.returncode

    def send_signal(self, sig):
        """Send signal ``sig`` to the process.

        On UNIX, ``sig`` may be any signal defined in the
        :mod:`signal` module, such as ``signal.SIGINT`` or
        ``signal.SIGTERM``. On Windows, it may be anything accepted by
        the standard library :meth:`subprocess.Popen.send_signal`.
        """
        if self.poll() is None:
            self._signals_sent_before_exit.add(sig)
            self._proc.send_signal(sig)

    def terminate(self):
        """Terminate the process, politely if possible.

        On UNIX, this is equivalent to
        ``send_signal(signal.SIGTERM)``; by convention this requests
        graceful termination, but a misbehaving or buggy process might
        ignore it. On Windows, :meth:`terminate` forcibly terminates the
        process in the same manner as :meth:`kill`.
        """
        if self.poll() is None:
            self._signals_sent_before_exit.add(signal.SIGTERM)
            self._proc.terminate()

    def kill(self):
        """Immediately terminate the process.

        On UNIX, this is equivalent to
        ``send_signal(signal.SIGKILL)``.  On Windows, it calls
        ``TerminateProcess``. In both cases, the process cannot
        prevent itself from being killed, but the termination will be
        delivered asynchronously; use :func:`wait` if you want to
        ensure the process is actually dead before proceeding.
        """
        if not hasattr(signal, "SIGKILL"):  # Windows
            self.terminate()
        elif self.poll() is None:
            self._signals_sent_before_exit.add(signal.SIGKILL)
            self._proc.kill()


async def run_process(
    command,
    *,
    input=None,
    check=True,
    timeout=None,
    deadline=None,
    shutdown_signal=None,
    shutdown_timeout=None,
    task_status=_core.TASK_STATUS_IGNORED,
    **options
):
    """Run ``command`` in a subprocess, wait for it to complete, and
    return a :class:`subprocess.CompletedProcess` instance describing
    the results.

    If cancelled, :func:`run_process` terminates the subprocess and waits for it
    to exit before propagating the cancellation, like :meth:`Process.aclose`.
    If you need to be able to tell what partial output the process produced
    before a timeout, see the ``timeout`` and ``deadline`` arguments.

    The default behavior of :func:`run_process` is designed to isolate
    the subprocess from potential impacts on the parent Trio process, and to
    reduce opportunities for errors to pass silently. Specifically:

    * The subprocess's standard input stream is set up to receive the
      bytes provided as ``input``.  Once the given input has been
      fully delivered, or if none is provided, the subprocess will
      receive end-of-file when reading from its standard input.

    * The subprocess's standard output and standard error streams are
      individually captured and returned as bytestrings from
      :func:`run_process`.

    * If the subprocess exits with a nonzero status code, indicating failure,
      :func:`run_process` raises a :exc:`subprocess.CalledProcessError`
      exception rather than returning normally. The captured outputs
      are still available as the ``stdout`` and ``stderr`` attributes
      of that exception.

    To suppress the :exc:`~subprocess.CalledProcessError` on failure,
    pass ``check=False``. To obtain different I/O behavior, use the
    lower-level ``stdin``, ``stdout``, and/or ``stderr``
    :ref:`subprocess options <subprocess-options>`, referred to
    collectively as ``stdX`` in the discussion below. It is an error to
    specify ``input`` if ``stdin`` is specified. If ``stdout`` or
    ``stderr`` is specified, the corresponding attribute of the
    returned :class:`~subprocess.CompletedProcess` object will be
    ``None``.

    By default, process termination uses a mechanism that cannot be
    intercepted by the process. If you want to allow the process to
    perform its own cleanup before exiting, you can specify a
    ``shutdown_signal``; then :func:`run_process` will send the
    process that signal instead of killing it outright, and will wait
    up to ``shutdown_timeout`` seconds (forever if unspecified) for
    the process to exit in response to that signal. If the shutdown
    timeout expires, the process gets forcibly killed.

    .. note:: If you pass ``stdX=subprocess.PIPE``
       explicitly, a pipe will be set up for the Trio process to
       communicate with that stream of the subprocess, but
       :func:`run_process` will not manage it. This allows you to
       start :func:`run_process` in the background, using
       ``nursery.start()`` to obtain the :class:`Process` object, and
       interact with the stream yourself. See :func:`interact_with_process`
       for a shortcut if you plan on doing this with multiple streams.

    .. note:: If you pass ``stdX=None``, that stream will not be
       redirected at all: the subprocess's output will go to the same
       place as the parent Trio process's output, likely the user's
       terminal. See :func:`delegate_to_process` for a shortcut if you
       plan on doing this with multiple streams.

    Args:
      command (str or list): The command to run. Typically this is a
          sequence of strings such as ``['ls', '-l', 'directory with spaces']``,
          where the first element names the executable to invoke and the other
          elements specify its arguments. It may also be a single string
          with space-separated words, in which case :ref:`quoting rules
          <subprocess-quoting>` come into play.
      input (bytes): The input to provide to the subprocess on its
          standard input stream. If you want the subprocess's input
          to come from something other than data specified at the time
          of the :func:`run_process` call, you can specify a redirection
          using the lower-level ``stdin`` option; then ``input`` must
          be unspecified or None.
      check (bool): If false, don't validate that the subprocess exits
          successfully. You should be sure to check the
          :attr:`Process.returncode` or :attr:`Process.failed`
          attribute of the returned object if you pass
          ``check=False``, so that errors don't pass silently.
      timeout (float): If specified, do not allow the process to run for
          longer than ``timeout`` seconds; if the timeout is reached,
          kill the subprocess and raise a :exc:`subprocess.TimeoutExpired`
          exception containing the output that was provided thus far.
      deadline (float): Like ``timeout``, but specified in terms of an
          absolute time on the current :ref:`clock <time-and-clocks>`
          at which to kill the process. It is an error to specify both
          ``timeout`` and ``deadline``.
      shutdown_signal (int): If specified, the process will be sent this
          signal when :func:`run_process` is cancelled or the given
          ``timeout`` or ``deadline`` expires, which might give it a chance
          to clean itself up before exiting. The default shutdown signal
          forcibly kills the process and cannot be caught. A value of zero
          will send no signal, just close pipes to the process when we want
          it to exit.
      shutdown_timeout (float): If specified, and the process does not
          exit within this many seconds after receiving the ``shutdown_signal``,
          it will be forcibly killed anyway. The default is to wait as long
          as it takes for the process to exit.
      task_status: This function can be used with ``nursery.start``.
          If it is, it returns the :class:`Process` object, so that other tasks
          can send signals to the subprocess or interact with streams created by
          ``stdX=subprocess.PIPE`` options while it runs.
      **options: :func:`run_process` also accepts any :ref:`general subprocess
          options <subprocess-options>` and passes them on to the
          :class:`~trio.Process` constructor.

    Returns:
      A :class:`subprocess.CompletedProcess` instance describing the
      return code and outputs.

    Raises:
      subprocess.TimeoutExpired: if the process is killed due to the
          expiry of the given ``deadline`` or ``timeout``
      subprocess.CalledProcessError: if ``check=False`` is not passed
          and the process exits with an exit status that we deem to constitute
          failure; see :attr:`Process.failed`
      OSError: if an error is encountered starting or communicating with
          the process

    """

    if 'stdin' in options:
        manage_stdin = False
        if input is not None:
            raise ValueError('stdin and input arguments may not both be used')
    else:
        manage_stdin = True
        options['stdin'] = subprocess.PIPE

    if 'stdout' in options:
        manage_stdout = False
    else:
        manage_stdout = True
        options['stdout'] = subprocess.PIPE

    if 'stderr' in options:
        manage_stderr = False
    else:
        manage_stderr = True
        options['stderr'] = subprocess.PIPE

    if timeout is not None and deadline is not None:
        raise ValueError('timeout and deadline arguments may not both be used')
    if timeout is not None:
        # Set deadline from timeout for ease of cancel scope management
        deadline = _core.current_time() + timeout
    elif deadline is not None:
        # Set timeout from deadline so we can include it in the exception
        timeout = deadline - _core.current_time()

    stdout_chunks = []
    stderr_chunks = []

    async def feed_input(stream):
        async with stream:
            if input:
                try:
                    await stream.send_all(input)
                except (_core.BrokenResourceError, _core.ClosedResourceError):
                    pass

    async def read_output(stream, chunks):
        async with stream:
            while True:
                try:
                    chunk = await stream.receive_some(32768)
                except _core.ClosedResourceError:
                    break
                if not chunk:
                    break
                chunks.append(chunk)

    async def manage_process(proc):
        async with _core.open_nursery() as nursery:
            if manage_stdin:
                nursery.start_soon(feed_input, proc.stdin)
            if manage_stdout:
                nursery.start_soon(read_output, proc.stdout, stdout_chunks)
            if manage_stderr:
                nursery.start_soon(read_output, proc.stderr, stderr_chunks)
            await proc.wait()

    async def shutdown_when_cancelled(proc, io_scope):
        # Wait for a cancellation, but capture it instead of propagating it.
        raise_cancel = None
        this_task = _core.current_task()

        def abort_fn(inner_raise_cancel):
            if proc.returncode is not None:
                # Process already exited -- propagate the cancellation
                io_scope.shield = False
                return _core.Abort.SUCCEEDED
            nonlocal raise_cancel
            raise_cancel = inner_raise_cancel
            _core.reschedule(this_task)
            return _core.Abort.FAILED

        await _core.wait_task_rescheduled(abort_fn)

        # We're in a cancelled scope at this point, so we shouldn't
        # execute any unshielded checkpoints.

        # Send shutdown signal, then wait up to the given shutdown_timeout
        # for the process to exit
        if shutdown_signal != 0:
            proc.send_signal(shutdown_signal)
        else:
            # shutdown_signal zero means "just close streams"
            with _core.open_cancel_scope(shield=True):
                if manage_stdin:
                    await proc.stdin.aclose()
                if manage_stdout:
                    await proc.stdout.aclose()
                if manage_stderr:
                    await proc.stderr.aclose()

        with _core.open_cancel_scope(shield=True) as shutdown_scope:
            if shutdown_timeout is not None:
                shutdown_scope.deadline = (
                    _core.current_time() + shutdown_timeout
                )
            await proc.wait()

        # If the process hasn't already exited at this point, let it die
        io_scope.shield = False
        raise_cancel()

    with _core.open_cancel_scope() as wait_scope:
        if deadline is not None:
            wait_scope.deadline = deadline

        async with Process(command, **options) as proc:
            task_status.started(proc)

            if shutdown_signal is None:
                await manage_process(proc)
            else:
                async with _core.open_nursery() as sentinel_nursery:
                    with _core.open_cancel_scope() as io_scope:
                        io_scope.shield = True
                        sentinel_nursery.start_soon(
                            shutdown_when_cancelled, proc, io_scope
                        )
                        await manage_process(proc)
                        sentinel_nursery.cancel_scope.cancel()

    stdout = b"".join(stdout_chunks) if manage_stdout else None
    stderr = b"".join(stderr_chunks) if manage_stderr else None

    if wait_scope.cancelled_caught:
        raise subprocess.TimeoutExpired(
            proc.args, timeout, output=stdout, stderr=stderr
        )
    if check and proc.failed:
        raise subprocess.CalledProcessError(
            proc.returncode, proc.args, output=stdout, stderr=stderr
        )

    return subprocess.CompletedProcess(
        proc.args, proc.returncode, stdout, stderr
    )


async def delegate_to_process(command, **run_options):
    """Run ``command`` in a subprocess, with its standard streams inherited
    from the parent Trio process (no redirection), and return a
    :exc:`subprocess.CompletedProcess` describing the results.

    This is useful, for example, if you want to spawn an interactive process
    and allow the user to interact with it. It is equivalent to
    ``functools.partial(run_process, stdin=None, stdout=None, stderr=None)``.

    .. note:: The child is run in the same process group as the
       parent, so on UNIX a user Ctrl+C will be delivered to the
       parent Trio process as well.  You may wish to block signals
       while the child is running, start it in a new process group, or
       start it in a pseudoterminal. Trio does not currently provide
       facilities for this.

    """
    run_options.setdefault("stdin", None)
    run_options.setdefault("stdout", None)
    run_options.setdefault("stderr", None)
    return await run_process(command, **run_options)


@attr.s(slots=True, frozen=True, cmp=False)
class ProcessStream(HalfCloseableStream):
    """A stream for communicating with a subprocess.

    Sending data on this stream writes it to the process's standard
    input, and receiving data receives from the process's standard
    output.  There is also an :attr:`errors` attribute which contains
    a :class:`~trio.abc.ReceiveStream` for reading from the process's
    standard error stream. If a stream is being redirected elsewhere,
    attempting to interact with it will raise :exc:`ClosedResourceError`.

    Closing a :class:`ProcessStream` closes all the underlying pipes,
    but unlike :meth:`Process.aclose`, it does *not* wait for the process
    to exit. You should use ``await stream.process.aclose()`` if you
    want the :meth:`Process.aclose` behavior.

    Args:
      process (:class:`Process`): The process to wrap.

    Attributes:
      process (:class:`Process`): The underlying process, so you can
          :meth:`send it signals <Process.send_signal>` or :meth:`~Process.wait`
          for it to exit.
      errors (:class:`~trio.abc.ReceiveStream`): A stream which reads from
          the underlying process's standard error if we have access to it,
          or raises :exc:`ClosedResourceError` on all reads if not.

    """

    process = attr.ib(type=Process)
    errors = attr.ib(type=ReceiveStream, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if self.process.errors is None:
            self.errors = NullStream()
            self.errors.close()
        else:
            self.errors = self.process.stderr

    async def aclose(self) -> None:
        await self.process.aclose()

    async def receive_some(self, max_bytes: int) -> bytes:
        if self.process.stdout is None:
            await _core.checkpoint()
            raise _core.ClosedResourceError(
                "can't read from process stdout that was redirected elsewhere"
            )
        return await self.process.stdout.receive_some(max_bytes)

    async def _stdin_operation(self, method, *args) -> None:
        if self.process.stdin is None:
            await _core.checkpoint()
            raise _core.ClosedResourceError(
                "can't write to process stdin that was redirected elsewhere"
            )
        await getattr(self.process.stdin, method)(*args)

    async def send_all(self, data: bytes) -> None:
        await self._stdin_operation("send_all", data)

    async def wait_send_all_might_not_block(self) -> None:
        await self._stdin_operation("wait_send_all_might_not_block")

    async def send_eof(self) -> None:
        await self._stdin_operation("send_eof")


@asynccontextmanager
@async_generator
async def interact_with_process(
    command,
    *,
    check=True,
    shutdown_signal=None,
    shutdown_timeout=None,
    **options
):
    """An async context manager that runs ``command`` in a subprocess and
    evaluates to a :class:`ProcessStream` that can be used to
    communicate with it.

    The subprocess is run as if by :func:`run_process` in a background
    task. If an exception propagates out of the ``async with`` block,
    the background :func:`run_process` task is cancelled, which kills
    the process (or sends the ``shutdown_signal`` if one has been
    specified). If the ``async with`` block exits normally, we close
    all pipes to and from the process and wait for it to exit on its own.

    If ``check=True`` (the default) and the subprocess exits with a
    failure indication, the body of the :func:`interact_with_process`
    context will be cancelled in order to propagate the
    :exc:`~subprocess.CalledProcessError`, and any exception raised from
    the body of the context will be left as the ``__context__`` of the
    :exc:`~subprocess.CalledProcessError`.

    By default, all three of the subprocess's standard streams are piped
    to the parent Trio process and available for use with the
    :class:`ProcessStream`. This may be overridden for a particular stream
    by specifying a different redirection for ``stdin``, ``stdout``, and/or
    ``stderr`` in the :ref:`subprocess options <subprocess-options>`.
    Attempting to interact with the child process's I/O streams in a manner
    that is not consistent with these redirections (such as trying to
    write data to a child spawned with ``stdin=subprocess.DEVNULL``)
    will raise a :exc:`ClosedResourceError`.

    Args:
      command (str or list): The command to run. Typically this is a
          sequence of strings such as ``['ls', '-l', 'directory with spaces']``,
          where the first element names the executable to invoke and the other
          elements specify its arguments. It may also be a single string
          with space-separated words, in which case :ref:`quoting rules
          <subprocess-quoting>` come into play.
      check (bool): If false, don't validate that the subprocess exits
          successfully. In this case you should be sure to check
          :attr:`stream.process.returncode <Process.returncode>` or
          :attr:`stream.process.failed <Process.failed>` upon exiting the
          context, so that errors don't pass silently.
      shutdown_signal (int): If specified, the process will be sent
          this signal when the :func:`interact_with_process` context
          exits with an exception or the surrounding scope becomes
          cancelled, which might give the process a chance to do some
          cleanup before exiting. The default shutdown signal forcibly
          kills the process and cannot be caught. A value of zero
          will send no signal, just close pipes to the process when we want
          it to exit.
      shutdown_timeout (float): If specified, and the process does not
          exit within this many seconds after receiving the ``shutdown_signal``,
          it will be forcibly killed anyway. The default is to wait as long
          as it takes for the process to exit.
      **options: :func:`interact_with_process` also accepts any
          :ref:`general subprocess options <subprocess-options>` and
          passes them on to the :class:`~trio.Process` constructor.

    """

    options.setdefault("stdin", subprocess.PIPE)
    options.setdefault("stdout", subprocess.PIPE)
    options.setdefault("stderr", subprocess.PIPE)

    process_error = None
    nested_child_error = None

    async def manage_process_in_background(cancel_scope, *, task_status):
        try:
            await run_process(
                command,
                check=check,
                shutdown_signal=shutdown_signal,
                shutdown_timeout=shutdown_timeout,
                **options
            )
        except subprocess.CalledProcessError as ex:
            nonlocal process_error
            process_error = ex
            cancel_scope.cancel()

    try:
        async with _core.open_nursery() as nursery:
            proc = await nursery.start(
                manage_process_in_background,
                nursery.cancel_scope,
                name="<subprocess: {!r}>".format(command)
            )
            async with ProcessStream(proc) as stream:
                await yield_(stream)

    except BaseException as process_context_error:
        if process_error is not None:
            raise
        try:
            raise process_error
        finally:
            sys.exc_info()[1].__context__ = process_context_error


    except BaseException as ex:
        proc_error = None

        def split_process_error(component):
            nonlocal proc_error
            if proc_error is not None:
                return component
            if isinstance(component, subprocess.CalledProcessError):
                if proc is not None and component.args is proc.args:
                    proc_error = component
                    return None
            return component

        # If a CalledProcessError and some other stuff propagated
        # at the same time, make the other stuff the context of the
        # CalledProcessError.
        ex = _core.MultiError.filter(split_process_error)
        if proc_error is not None:
            try:
                raise proc_error from None
            finally:
                sys.exc_info()[1].__context__ = ex

        # Otherwise raise the exception unchanged
        raise
