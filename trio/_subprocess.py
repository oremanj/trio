import math
import os
import select
import subprocess
import sys
import attr

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

    """

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
        self._proc.send_signal(sig)

    def terminate(self):
        """Forwards to :meth:`subprocess.Popen.terminate`."""
        self._proc.terminate()

    def kill(self):
        """Forwards to :meth:`subprocess.Popen.kill`."""
        self._proc.kill()


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

    If cancelled, :func:`run_process` kills the subprocess and waits for it
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
    collectively ``std__`` in the discussion below. It is an error to
    specify ``input`` if ``stdin`` is specified. If ``stdout`` or
    ``stderr`` is specified, the corresponding attribute of the
    returned :class:`~subprocess.CompletedProcess` object will be
    ``None``.

    .. note:: If you pass ``std__=subprocess.PIPE``
       explicitly, a pipe will be set up for the Trio process to
       communicate with that stream of the subprocess, but
       :func:`run_process` will not manage it. This allows you to
       start :func:`run_process` in the background, using
       ``nursery.start()`` to obtain the :class:`Process` object, and
       interact with the stream yourself. See :func:`interact_with_process`
       for a shortcut if you plan on doing this with multiple streams.

    .. note:: If you pass ``std__=None``, that stream will not be
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
      check (bool): If false, don't validate that the subprocess
          returns an exit status of zero (success). You should be sure
          to check the ``returncode`` attribute of the returned object
          if you pass ``check=False``, so that errors don't pass silently.
      timeout (float): If specified, do not allow the process to run for
          longer than ``timeout`` seconds; if the timeout is reached,
          kill the subprocess and raise a :exc:`subprocess.TimeoutExpired`
          exception containing the output that was provided thus far.
      deadline (float): Like ``timeout``, but specified in terms of an
          absolute time on the current :ref:`clock <time-and-clocks>`
          at which to kill the process. It is an error to specify both
          ``timeout`` and ``deadline``.
      task_status: This function can be used with ``nursery.start``,
          and provides the :class:`Process` object, so that other tasks
          can send it signals or interact with streams created by
          ``std__=subprocess.PIPE`` options.
      **options: :func:`run_process` also accepts any :ref:`general subprocess
          options <subprocess-options>` and passes them onto the
          :class:`~trio.subprocess.Process` constructor.

    Returns:
      A :class:`subprocess.CompletedProcess` instance describing the
      return code and outputs.

    Raises:
      subprocess.TimeoutExpired: if the process is killed due to the
          expiry of the given ``deadline`` or ``timeout``
      subprocess.CalledProcessError: if ``check=False`` is not passed
          and the process exits with a nonzero exit status
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

    stdout_chunks = []
    stderr_chunks = []

    async with Process(command, **options) as proc:
        task_status.started(proc)

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
            if manage_stdin:
                nursery.start_soon(feed_input)
            if manage_stdout:
                nursery.start_soon(read_output, proc.stdout, stdout_chunks)
            if manage_stderr:
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

    stdout = b"".join(stdout_chunks) if manage_stdout else None
    stderr = b"".join(stderr_chunks) if manage_stderr else None

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
    attempts to interact with it will raise :exc:`ClosedResourceError`.

    Args:
      process (:class:`Process`): The process to wrap.

    Attributes:
      process (:class:`Process`): The underlying process, so you can
          :meth:`send it signals <~Process.send_signal>` or :meth:`~Process.wait`
          for it to exit.
      errors (:class:`~trio.abc.ReceiveStream`): A stream which reads from
          the underlying process's standard error if we have access to it,
          or raises :exc:`ClosedResourceError` on all reads if not.

    """

    process = attr.ib(type=Process)
    stderr = attr.ib(type=ReceiveStream, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if self.process.stderr is None:
            self.stderr = NullStream()
            self.stderr.close()
        else:
            self.stderr = self.process.stderr

    async def _stdin_operation(self, method, *args) -> None:
        if self.process.stdin is None:
            await _core.checkpoint()
            raise _core.ClosedResourceError(
                "can't write to process stdin that was redirected elsewhere"
            )
        try:
            await getattr(self.process.stdin, method)(*args)
        except _core.BrokenResourceError as exc:
            exc._for_process_stream = self
            raise

    async def aclose(self) -> None:
        await self.process.aclose()

    async def receive_some(self, max_bytes: int) -> bytes:
        if self.process.stdout is None:
            await _core.checkpoint()
            return b""
        return await self.process.stdout.receive_some(max_bytes)

    async def send_all(self, data: bytes) -> None:
        await self._stdin_operation("send_all", data)

    async def wait_send_all_might_not_block(self) -> None:
        await self._stdin_operation("wait_send_all_might_not_block")

    async def send_eof(self) -> None:
        await self._stdin_operation("send_eof")


@asynccontextmanager
@async_generator
async def interact_with_process(
    command, *, check=True, cancel_on_child_exit=True, **options
):
    """An async context manager that runs ``command`` in a subprocess and
    evaluates to a :class:`~trio.abc.HalfCloseableStream` that can be used to
    communicate with it.

    The behavior of::

        async with trio.open_stream_to_process(command, **options) as stream:
            await do_the_thing(stream)

    is equivalent to::

        async with trio.Process(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, **options
        ) as proc:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(proc.wait)
                await do_the_thing(trio.ProcessStream(proc))
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, proc.args)

    except that if you use :func:`open_stream_to_process`, ``do_the_thing`` will
    be i

    The lifetime of the subprocess is scoped within the context manager, as if
    
    Exiting the scope closes the pipes with the process and waits for it
    to exit
    """
