import math
import os
import select
import subprocess
import sys

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
    After construction, you can interact with the child process
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

    :class:`Process` implements :class:`~trio.abc.AsyncResource`,
    so you can use it as an async context manager or call its
    :meth:`aclose` method directly. "Closing" a :class:`Process`
    will close any pipes to the child and wait for it to exit;
    if cancelled, the child will be forcibly killed and we will
    ensure it has finished exiting before allowing the cancellation
    to propagate. It is *strongly recommended* that process lifetime
    be scoped using an ``async with`` block wherever possible, to
    avoid winding up with processes hanging around longer than you
    were planning on.

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
      was_signaled (bool): True if the child proess managed by this object
          received a signal due to the :meth:`~Process.kill`,
          :meth:`~Process.terminate`, or :meth:`~Procwss.send_signal`
          method being called.

    """

    was_signaled = False
    universal_newlines = False
    encoding = None
    errors = None

    # Available for the per-platform wait_child_exiting() implementations
    # to stash some state; waitid platforms use this to avoid spawning
    # arbitrarily many threads if wait() keeps getting cancelled.
    _wait_for_exit_data = None

    def __init__(
        self, args, *, stdin=None, stdout=None, stderr=None, **options
    ):
        for key in (
            'universal_newlines', 'text', 'encoding', 'errors', 'bufsize'
        ):
            if options.get(key):
                raise TypeError(
                    "trio.subprocess.Process only supports communicating over "
                    "unbuffered byte streams; the '{}' option is not supported"
                    .format(key)
                )

        self.stdin = None
        self.stdout = None
        self.stderr = None

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
                args, stdin=stdin, stdout=stdout, stderr=stderr, **options
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
        elif self.returncode < 0:
            status = "exited with signal {}".format(-self.returncode)
        else:
            status = "exited with status {}".format(self.returncode)
        return "<trio.Process {!r}: {}>".format(self.args, self.status)

    @property
    def returncode(self):
        """The exit status of the process (an integer), or ``None`` if it has
        not exited.

        Negative values indicate termination due to a signal (on UNIX only).
        Like :attr:`subprocess.Popen.returncode`, this is not updated outside
        of a call to :meth:`wait` or :meth:`poll`.
        """
        return self._proc.returncode

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

    async def wait(self):
        """Block until the process exits.

        Returns:
          The exit status of the process (a nonnegative integer, with
          zero usually indicating success). On UNIX systems, a process
          that exits due to a signal will have its exit status reported
          as the negative of that signal number, e.g., -11 for ``SIGSEGV``.
        """
        if self.poll() is None:
            await wait_child_exiting(self)
            self._proc.wait()
        else:
            await _core.checkpoint()
        return self.returncode

    def poll(self):
        """Forwards to :meth:`subprocess.Popen.poll`."""
        return self._proc.poll()

    def send_signal(self, sig):
        """Forwards to :meth:`subprocess.Popen.send_signal`."""
        self.was_signaled = True
        self._proc.send_signal(sig)

    def terminate(self):
        """Forwards to :meth:`subprocess.Popen.terminate`."""
        self.was_signaled = True
        self._proc.terminate()

    def kill(self):
        """Forwards to :meth:`subprocess.Popen.kill`."""
        self.was_signaled = True
        self._proc.kill()


@attr.s(slots=True, frozen=True, cmp=False)
class ProcessStream(HalfCloseableStream):
    """Wraps a :class:`Process` in a :class:`~trio.abc.HalfCloseableStream`
    interface.

    Sending data on this stream writes it to the process's standard input,
    and receiving data receives from the process's standard output.
    There is also an :attr:`errors` attribute which provides a
    :class:`~trio.abc.ReceiveStream` for reading from the process's standard
    error stream. If the process's standard error is not being piped to us,
    :attr:`errors` evaluates to a :class:`~trio.NullStream` which will
    immediately return end-of-file on any reads.

    Args:
      process (:class:`Process`): The process to wrap. Its standard output and
          standard input must have been set up to be piped to and from
          the Trio process; its standard error may or may not be piped.

    Raises:
      ValueError: if the given process's standard input or output streams
          are not piped to the Trio process

    Attributes:
      process (:class:`Process`): The underlying process, so you can
          :meth:`send it signals <~Process.send_signal>` or :meth:`~Process.wait`
          for it to exit.
      errors (:class:`~trio.abc.ReceiveStream`): A stream which reads from
          the underlying process's standard error if we have access to it,
          or returns EOF to all reads if not.
    """

    process = attr.ib(type=Process)
    stderr = attr.ib(type=ReceiveStream, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if self.process.stdin is None or self.process.stdout is None:
            raise ValueError(
                "ProcessStream can only wrap a Process whose stdin and stdout "
                "are piped to the parent"
            )
        if self.process.stderr is None:
            self.stderr = NullStream()
        else:
            self.stderr = self.process.stderr

    async def aclose(self) -> None:
        await self.process.aclose()

    async def receive_some(self, max_bytes: int) -> bytes:
        return await self.process.stdout.receive_some(max_bytes)

    async def send_all(self, data: bytes) -> None:
        await self.process.stdin.send_all(data)

    async def wait_send_all_might_not_block(self) -> None:
        await self.process.stdin.wait_send_all_might_not_block()

    async def send_eof(self) -> None:
        await self.process.stdin.aclose()


async def run_process(
    command,
    *,
    input=None,
    check=True,
    timeout=None,
    deadline=None,
    task_status=_core.TASK_STATUS_IGNORED,
    **options
):
    """Run ``command`` in a subprocess, wait for it to complete, and
    return a :class:`subprocess.CompletedProcess` instance describing
    the results.

    If cancelled, :func:`run` kills the subprocess and waits for it
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
    :ref:`subprocess options <subprocess-options>`.  It is an error to
    specify ``input`` if ``stdin`` is specified as something other
    than ``subprocess.PIPE``. If ``stdout`` or ``stderr`` is specified
    as something other than ``subprocess.PIPE``, the corresponding
    attribute of the returned :class:`~subprocess.CompletedProcess`
    object will be ``None``.

    Args:
      command (str or list): The command to run. Typically this is a
          sequence of strings such as ``['ls', '-l', 'directory with spaces']``,
          where the first element names the executable to invoke and the other
          elements specify its arguments. If ``shell=True`` is given as
          an option, ``command`` should be a single string like
          ``"ls -l 'directory with spaces'"``, which will
          split into words following the shell's quoting rules.
      input (bytes): If specified, set up the subprocess to
          read its standard input stream from a pipe, and feed that
          pipe with the given bytes while the subprocess is running.
          Once the supplied input is exhausted, the pipe will be
          closed so that the subprocess receives an end-of-file
          indication. Note that ``input=b""`` and ``input=None``
          behave differently: ``input=b""`` sets up the subprocess
          to read its input from a pipe with no data in it, while
          ``input=None`` performs no input redirection at all, so
          that the subprocess's standard input stream is the same
          as the parent process's.
      capture_output (bool): If true, set up the subprocess to write
          its standard output and standard error streams to pipes,
          the contents of which will be consumed in the parent process
          and ultimately rendered as the ``stdout`` and ``stderr``
          attributes of the returned :class:`~subprocess.CompletedProcess`
          object. (This argument is supported by Trio on all
          Python versions, but was only added to the standard library
          in 3.7.)
      check (bool): If true, validate that the subprocess returns an exit
          status of zero (success). Any nonzero exit status will be
          converted into a :exc:`subprocess.CalledProcessError`
          exception.
      timeout (float): If specified, do not allow the process to run for
          longer than ``timeout`` seconds; if the timeout is reached,
          kill the subprocess and raise a :exc:`subprocess.TimeoutExpired`
          exception containing the output that was provided thus far.
      deadline (float): Like ``timeout``, but specified in terms of an
          absolute time on the current :ref:`clock <time-and-clocks>`
          at which to kill the process. It is an error to specify both
          ``timeout`` and ``deadline``.
      **options: :func:`run` also accepts any :ref:`general subprocess
          options <subprocess-options>` and passes them onto the
          :class:`~trio.subprocess.Process` constructor. It is an
          error to specify ``stdin`` if ``input`` was also specified,
          or to specify ``stdout`` or ``stderr`` if ``capture_output``
          was also specified.

    Returns:
      A :class:`subprocess.CompletedProcess` instance describing the
      return code and outputs.

    Raises:
      subprocess.TimeoutExpired: if the process is killed due to timeout
          expiry
      subprocess.CalledProcessError: if check=True is passed and the process
          exits with a nonzero exit status
      OSError: if an error is encountered starting or communicating with
          the process

    See also:
      :func:`delegate_to_process`, :func:`open_stream_to_process`,
      :class:`Process`

    If not otherwise redirected using the ``stdout=`` or ``stderr=``
    ,  If you want to read the
    subprocess's output in a streaming fashion, see
    :func:`open_stream_to_process`.

    By default, if the subprocess exits with any indication other
    than zero (success), a :exc:`subprocess.CalledProcessError` is
    raised. The captured outputs, if any, are available as ``stdout``
    and ``stderr`` attributes on this exception.

    """
    if input is not None:
        if 'stdin' in options:
            raise ValueError('stdin and input arguments may not both be used')
        options['stdin'] = subprocess.PIPE

    if capture_output:
        if 'stdout' in options or 'stderr' in options:
            raise ValueError(
                'capture_output and stdout/stderr arguments may not both be used'
            )
        options['stdout'] = options['stderr'] = subprocess.PIPE

    if timeout is not None and deadline is not None:
        raise ValueError('timeout and deadline arguments may not both be used')

    stdout_chunks = []
    stderr_chunks = []

    async with Process(command, **options) as proc:

        async def feed_input():
            async with proc.stdin:
                if input:
                    try:
                        await proc.stdin.send_all(input)
                    except _core.BrokenResourceError:
                        pass

        async def read_output(stream, chunks):
            async with stream:
                while True:
                    chunk = await stream.receive_some(32768)
                    if not chunk:
                        break
                    chunks.append(chunk)

        async with _core.open_nursery() as nursery:
            if proc.stdin is not None:
                nursery.start_soon(feed_input)
            if proc.stdout is not None:
                nursery.start_soon(read_output, proc.stdout, stdout_chunks)
            if proc.stderr is not None:
                nursery.start_soon(read_output, proc.stderr, stderr_chunks)

            with _core.open_cancel_scope() as wait_scope:
                if timeout is not None:
                    wait_scope.deadline = _core.current_time() + timeout
                if deadline is not None:
                    wait_scope.deadline = deadline
                    timeout = deadline - _core.current_time()
                await proc.wait()

            if wait_scope.cancelled_caught:
                proc.kill()
                nursery.cancel_scope.cancel()

    stdout = b"".join(stdout_chunks) if proc.stdout is not None else None
    stderr = b"".join(stderr_chunks) if proc.stderr is not None else None

    if wait_scope.cancelled_caught:
        raise subprocess.TimeoutExpired(
            proc.args, timeout, output=stdout, stderr=stderr
        )
    if check and proc.returncode:
        raise subprocess.CalledProcessError(
            proc.returncode, proc.args, output=stdout, stderr=stderr
        )

    return subprocess.CompletedProcess(
        proc.args, proc.returncode, stdout, stderr
    )
