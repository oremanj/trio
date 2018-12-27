.. currentmodule:: trio

I/O in Trio
===========

.. _abstract-stream-api:

The abstract Stream API
-----------------------

Trio provides a set of abstract base classes that define a standard
interface for unidirectional and bidirectional byte streams.

Why is this useful? Because it lets you write generic protocol
implementations that can work over arbitrary transports, and easily
create complex transport configurations. Here's some examples:

* :class:`trio.SocketStream` wraps a raw socket (like a TCP connection
  over the network), and converts it to the standard stream interface.

* :class:`trio.ssl.SSLStream` is a "stream adapter" that can take any
  object that implements the :class:`trio.abc.Stream` interface, and
  convert it into an encrypted stream. In trio the standard way to
  speak SSL over the network is to wrap an
  :class:`~trio.ssl.SSLStream` around a :class:`~trio.SocketStream`.

* If you spawn a :ref:`subprocess`, you can get a
  :class:`~trio.abc.SendStream` that lets you write to its stdin, and
  a :class:`~trio.abc.ReceiveStream` that lets you read from its
  stdout. If for some reason you wanted to speak SSL to a subprocess,
  you could use a :class:`StapledStream` to combine its stdin/stdout
  into a single bidirectional :class:`~trio.abc.Stream`, and then wrap
  that in an :class:`~trio.ssl.SSLStream`::

     ssl_context = trio.ssl.create_default_context()
     ssl_context.check_hostname = False
     s = SSLStream(StapledStream(process.stdin, process.stdout), ssl_context)

* It sometimes happens that you want to connect to an HTTPS server,
  but you have to go through a web proxy... and the proxy also uses
  HTTPS. So you end up having to do `SSL-on-top-of-SSL
  <https://daniel.haxx.se/blog/2016/11/26/https-proxy-with-curl/>`__. In
  trio this is trivial – just wrap your first
  :class:`~trio.ssl.SSLStream` in a second
  :class:`~trio.ssl.SSLStream`::

     # Get a raw SocketStream connection to the proxy:
     s0 = await open_tcp_stream("proxy", 443)

     # Set up SSL connection to proxy:
     s1 = SSLStream(s0, proxy_ssl_context, server_hostname="proxy")
     # Request a connection to the website
     await s1.send_all(b"CONNECT website:443 / HTTP/1.0\r\n\r\n")
     await check_CONNECT_response(s1)

     # Set up SSL connection to the real website. Notice that s1 is
     # already an SSLStream object, and here we're wrapping a second
     # SSLStream object around it.
     s2 = SSLStream(s1, website_ssl_context, server_hostname="website")
     # Make our request
     await s2.send_all(b"GET /index.html HTTP/1.0\r\n\r\n")
     ...

* The :mod:`trio.testing` module provides a set of :ref:`flexible
  in-memory stream object implementations <testing-streams>`, so if
  you have a protocol implementation to test then you can can start
  two tasks, set up a virtual "socket" connecting them, and then do
  things like inject random-but-repeatable delays into the connection.


Abstract base classes
~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: trio.abc

.. http://docutils.sourceforge.net/docs/ref/rst/directives.html#list-table

.. list-table:: Overview: abstract base classes for I/O
   :widths: auto
   :header-rows: 1

   * - Abstract base class
     - Inherits from...
     - Adds these abstract methods...
     - And these concrete methods.
     - Example implementations
   * - :class:`AsyncResource`
     -
     - :meth:`~AsyncResource.aclose`
     - ``__aenter__``, ``__aexit__``
     - :ref:`async-file-objects`
   * - :class:`SendStream`
     - :class:`AsyncResource`
     - :meth:`~SendStream.send_all`,
       :meth:`~SendStream.wait_send_all_might_not_block`
     -
     - :class:`~trio.testing.MemorySendStream`
   * - :class:`ReceiveStream`
     - :class:`AsyncResource`
     - :meth:`~ReceiveStream.receive_some`
     -
     - :class:`~trio.testing.MemoryReceiveStream`
   * - :class:`Stream`
     - :class:`SendStream`, :class:`ReceiveStream`
     -
     -
     - :class:`~trio.ssl.SSLStream`
   * - :class:`HalfCloseableStream`
     - :class:`Stream`
     - :meth:`~HalfCloseableStream.send_eof`
     -
     - :class:`~trio.SocketStream`, :class:`~trio.StapledStream`
   * - :class:`Listener`
     - :class:`AsyncResource`
     - :meth:`~Listener.accept`
     -
     - :class:`~trio.SocketListener`, :class:`~trio.ssl.SSLListener`
   * - :class:`SendChannel`
     - :class:`AsyncResource`
     - :meth:`~SendChannel.send`, :meth:`~SendChannel.send_nowait`
     -
     - :func:`~trio.open_memory_channel`
   * - :class:`ReceiveChannel`
     - :class:`AsyncResource`
     - :meth:`~ReceiveChannel.receive`, :meth:`~ReceiveChannel.receive_nowait`
     - ``__aiter__``, ``__anext__``
     - :func:`~trio.open_memory_channel`

.. autoclass:: trio.abc.AsyncResource
   :members:

.. currentmodule:: trio

.. autofunction:: aclose_forcefully

.. currentmodule:: trio.abc

.. autoclass:: trio.abc.SendStream
   :members:
   :show-inheritance:

.. autoclass:: trio.abc.ReceiveStream
   :members:
   :show-inheritance:

.. autoclass:: trio.abc.Stream
   :members:
   :show-inheritance:

.. autoclass:: trio.abc.HalfCloseableStream
   :members:
   :show-inheritance:

.. currentmodule:: trio.abc

.. autoclass:: trio.abc.Listener
   :members:
   :show-inheritance:

.. autoclass:: trio.abc.SendChannel
   :members:
   :show-inheritance:

.. autoclass:: trio.abc.ReceiveChannel
   :members:
   :show-inheritance:

.. currentmodule:: trio


Generic stream tools
~~~~~~~~~~~~~~~~~~~~

Trio currently provides a generic helper for writing servers that
listen for connections using one or more
:class:`~trio.abc.Listener`\s, and two generic utility classes for working
with streams. And if you want to test code that's written against the
streams interface, you should also check out :ref:`testing-streams` in
:mod:`trio.testing`.

.. autofunction:: serve_listeners

.. autoclass:: StapledStream
   :members:
   :show-inheritance:

.. autoclass:: NullStream
   :show-inheritance:


.. _high-level-networking:

Sockets and networking
~~~~~~~~~~~~~~~~~~~~~~

The high-level network interface is built on top of our stream
abstraction.

.. autofunction:: open_tcp_stream

.. autofunction:: serve_tcp

.. autofunction:: open_ssl_over_tcp_stream

.. autofunction:: serve_ssl_over_tcp

.. autofunction:: open_unix_socket

.. autoclass:: SocketStream
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SocketListener
   :members:
   :show-inheritance:

.. autofunction:: open_tcp_listeners

.. autofunction:: open_ssl_over_tcp_listeners


SSL / TLS support
~~~~~~~~~~~~~~~~~

.. module:: trio.ssl

The :mod:`trio.ssl` module implements SSL/TLS support for Trio, using
the standard library :mod:`ssl` module. It re-exports most of
:mod:`ssl`\´s API, with the notable exception of
:class:`ssl.SSLContext`, which has unsafe defaults; if you really want
to use :class:`ssl.SSLContext` you can import it from :mod:`ssl`, but
normally you should create your contexts using
:func:`trio.ssl.create_default_context <ssl.create_default_context>`.

Instead of using :meth:`ssl.SSLContext.wrap_socket`, though, you
create a :class:`SSLStream`:

.. autoclass:: SSLStream
   :show-inheritance:
   :members:

And if you're implementing a server, you can use :class:`SSLListener`:

.. autoclass:: SSLListener
   :show-inheritance:
   :members:


.. module:: trio.socket

Low-level networking with :mod:`trio.socket`
---------------------------------------------

The :mod:`trio.socket` module provides trio's basic low-level
networking API. If you're doing ordinary things with stream-oriented
connections over IPv4/IPv6/Unix domain sockets, then you probably want
to stick to the high-level API described above. If you want to use
UDP, or exotic address families like ``AF_BLUETOOTH``, or otherwise
get direct access to all the quirky bits of your system's networking
API, then you're in the right place.


Top-level exports
~~~~~~~~~~~~~~~~~

Generally, the API exposed by :mod:`trio.socket` mirrors that of the
standard library :mod:`socket` module. Most constants (like
``SOL_SOCKET``) and simple utilities (like :func:`~socket.inet_aton`)
are simply re-exported unchanged. But there are also some differences,
which are described here.

First, Trio provides analogues to all the standard library functions
that return socket objects; their interface is identical, except that
they're modified to return trio socket objects instead:

.. autofunction:: socket

.. autofunction:: socketpair

.. autofunction:: fromfd

.. function:: fromshare(data)

   Like :func:`socket.fromshare`, but returns a trio socket object.

In addition, there is a new function to directly convert a standard
library socket into a trio socket:

.. autofunction:: from_stdlib_socket

Unlike :func:`socket.socket`, :func:`trio.socket.socket` is a
function, not a class; if you want to check whether an object is a
trio socket, use ``isinstance(obj, trio.socket.SocketType)``.

For name lookup, Trio provides the standard functions, but with some
changes:

.. autofunction:: getaddrinfo

.. autofunction:: getnameinfo

.. autofunction:: getprotobyname

Trio intentionally DOES NOT include some obsolete, redundant, or
broken features:

* :func:`~socket.gethostbyname`, :func:`~socket.gethostbyname_ex`,
  :func:`~socket.gethostbyaddr`: obsolete; use
  :func:`~socket.getaddrinfo` and :func:`~socket.getnameinfo` instead.

* :func:`~socket.getservbyport`: obsolete and `buggy
  <https://bugs.python.org/issue30482>`__; instead, do::

     _, service_name = await getnameinfo((127.0.0.1, port), NI_NUMERICHOST))

* :func:`~socket.getservbyname`: obsolete and `buggy
  <https://bugs.python.org/issue30482>`__; instead, do::

     await getaddrinfo(None, service_name)

* :func:`~socket.getfqdn`: obsolete; use :func:`getaddrinfo` with the
  ``AI_CANONNAME`` flag.

* :func:`~socket.getdefaulttimeout`,
  :func:`~socket.setdefaulttimeout`: instead, use trio's standard
  support for :ref:`cancellation`.

* On Windows, ``SO_REUSEADDR`` is not exported, because it's a trap:
  the name is the same as Unix ``SO_REUSEADDR``, but the semantics are
  `different and extremely broken
  <https://msdn.microsoft.com/en-us/library/windows/desktop/ms740621(v=vs.85).aspx>`__. In
  the very rare cases where you actually want ``SO_REUSEADDR`` on
  Windows, then it can still be accessed from the standard library's
  :mod:`socket` module.


Socket objects
~~~~~~~~~~~~~~

.. class:: SocketType

   .. note:: :class:`trio.socket.SocketType` is an abstract class and
      cannot be instantiated directly; you get concrete socket objects
      by calling constructors like :func:`trio.socket.socket`.
      However, you can use it to check if an object is a Trio socket
      via ``isinstance(obj, trio.socket.SocketType)``.

   Trio socket objects are overall very similar to the :ref:`standard
   library socket objects <python:socket-objects>`, with a few
   important differences:

   First, and most obviously, everything is made "trio-style":
   blocking methods become async methods, and the following attributes
   are *not* supported:

   * :meth:`~socket.socket.setblocking`: trio sockets always act like
     blocking sockets; if you need to read/write from multiple sockets
     at once, then create multiple tasks.
   * :meth:`~socket.socket.settimeout`: see :ref:`cancellation` instead.
   * :meth:`~socket.socket.makefile`: Python's file-like API is
     synchronous, so it can't be implemented on top of an async
     socket.
   * :meth:`~socket.socket.sendall`: Could be supported, but you're
     better off using the higher-level
     :class:`~trio.SocketStream`, and specifically its
     :meth:`~trio.SocketStream.send_all` method, which also does
     additional error checking.

   In addition, the following methods are similar to the equivalents
   in :func:`socket.socket`, but have some trio-specific quirks:

   .. method:: connect
      :async:

      Connect the socket to a remote address.

      Similar to :meth:`socket.socket.connect`, except async.

      .. warning::

         Due to limitations of the underlying operating system APIs, it is
         not always possible to properly cancel a connection attempt once it
         has begun. If :meth:`connect` is cancelled, and is unable to
         abort the connection attempt, then it will:

         1. forcibly close the socket to prevent accidental re-use
         2. raise :exc:`~trio.Cancelled`.

         tl;dr: if :meth:`connect` is cancelled then the socket is
         left in an unknown state – possibly open, and possibly
         closed. The only reasonable thing to do is to close it.

   .. method:: sendfile

      `Not implemented yet! <https://github.com/python-trio/trio/issues/45>`__

   We also keep track of an extra bit of state, because it turns out
   to be useful for :class:`trio.SocketStream`:

   .. attribute:: did_shutdown_SHUT_WR

      This :class:`bool` attribute is True if you've called
      ``sock.shutdown(SHUT_WR)`` or ``sock.shutdown(SHUT_RDWR)``, and
      False otherwise.

   The following methods are identical to their equivalents in
   :func:`socket.socket`, except async, and the ones that take address
   arguments require pre-resolved addresses:

   * :meth:`~socket.socket.accept`
   * :meth:`~socket.socket.recv`
   * :meth:`~socket.socket.recv_into`
   * :meth:`~socket.socket.recvfrom`
   * :meth:`~socket.socket.recvfrom_into`
   * :meth:`~socket.socket.recvmsg` (if available)
   * :meth:`~socket.socket.recvmsg_into` (if available)
   * :meth:`~socket.socket.send`
   * :meth:`~socket.socket.sendto`
   * :meth:`~socket.socket.sendmsg` (if available)

   All methods and attributes *not* mentioned above are identical to
   their equivalents in :func:`socket.socket`:

   * :attr:`~socket.socket.family`
   * :attr:`~socket.socket.type`
   * :attr:`~socket.socket.proto`
   * :meth:`~socket.socket.fileno`
   * :meth:`~socket.socket.listen`
   * :meth:`~socket.socket.getpeername`
   * :meth:`~socket.socket.getsockname`
   * :meth:`~socket.socket.close`
   * :meth:`~socket.socket.shutdown`
   * :meth:`~socket.socket.setsockopt`
   * :meth:`~socket.socket.getsockopt`
   * :meth:`~socket.socket.dup`
   * :meth:`~socket.socket.detach`
   * :meth:`~socket.socket.share`
   * :meth:`~socket.socket.set_inheritable`
   * :meth:`~socket.socket.get_inheritable`

.. currentmodule:: trio


.. _async-file-io:

Asynchronous filesystem I/O
---------------------------

Trio provides built-in facilities for performing asynchronous
filesystem operations like reading or renaming a file. Generally, we
recommend that you use these instead of Python's normal synchronous
file APIs. But the tradeoffs here are somewhat subtle: sometimes
people switch to async I/O, and then they're surprised and confused
when they find it doesn't speed up their program. The next section
explains the theory behind async file I/O, to help you better
understand your code's behavior. Or, if you just want to get started,
you can `jump down to the API overview
<ref:async-file-io-overview>`__.


Background: Why is async file I/O useful? The answer may surprise you
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many people expect that switching to from synchronous file I/O to
async file I/O will always make their program faster. This is not
true! If we just look at total throughput, then async file I/O might
be faster, slower, or about the same, and it depends in a complicated
way on things like your exact patterns of disk access, or how much RAM
you have. The main motivation for async file I/O is not to improve
throughput, but to **reduce the frequency of latency glitches.**

To understand why, you need to know two things.

First, right now no mainstream operating system offers a generic,
reliable, native API for async file or filesystem operations, so we
have to fake it by using threads (specifically,
:func:`run_sync_in_worker_thread`). This is cheap but isn't free: on a
typical PC, dispatching to a worker thread adds something like ~100 µs
of overhead to each operation. ("µs" is pronounced "microseconds", and
there are 1,000,000 µs in a second. Note that all the numbers here are
going to be rough orders of magnitude to give you a sense of scale; if
you need precise numbers for your environment, measure!)

.. file.read benchmark is notes-to-self/file-read-latency.py
.. Numbers for spinning disks and SSDs are from taking a few random
   recent reviews from http://www.storagereview.com/best_drives and
   looking at their "4K Write Latency" test results for "Average MS"
   and "Max MS":
   http://www.storagereview.com/samsung_ssd_850_evo_ssd_review
   http://www.storagereview.com/wd_black_6tb_hdd_review

And second, the cost of a disk operation is incredibly
bimodal. Sometimes, the data you need is already cached in RAM, and
then accessing it is very, very fast – calling :class:`io.FileIO`\'s
``read`` method on a cached file takes on the order of ~1 µs. But when
the data isn't cached, then accessing it is much, much slower: the
average is ~100 µs for SSDs and ~10,000 µs for spinning disks, and if
you look at tail latencies then for both types of storage you'll see
cases where occasionally some operation will be 10x or 100x slower
than average. And that's assuming your program is the only thing
trying to use that disk – if you're on some oversold cloud VM fighting
for I/O with other tenants then who knows what will happen. And some
operations can require multiple disk accesses.

Putting these together: if your data is in RAM then it should be clear
that using a thread is a terrible idea – if you add 100 µs of overhead
to a 1 µs operation, then that's a 100x slowdown! On the other hand,
if your data's on a spinning disk, then using a thread is *great* –
instead of blocking the main thread and all tasks for 10,000 µs, we
only block them for 100 µs and can spend the rest of that time running
other tasks to get useful work done, which can effectively be a 100x
speedup.

But here's the problem: for any individual I/O operation, there's no
way to know in advance whether it's going to be one of the fast ones
or one of the slow ones, so you can't pick and choose. When you switch
to async file I/O, it makes all the fast operations slower, and all
the slow operations faster. Is that a win? In terms of overall speed,
it's hard to say: it depends what kind of disks you're using and your
kernel's disk cache hit rate, which in turn depends on your file
access patterns, how much spare RAM you have, the load on your
service, ... all kinds of things. If the answer is important to you,
then there's no substitute for measuring your code's actual behavior
in your actual deployment environment. But what we *can* say is that
async disk I/O makes performance much more predictable across a wider
range of runtime conditions.

**If you're not sure what to do, then we recommend that you use async
disk I/O by default,** because it makes your code more robust when
conditions are bad, especially with regards to tail latencies; this
improves the chances that what your users see matches what you saw in
testing. Blocking the main thread stops *all* tasks from running for
that time. 10,000 µs is 10 ms, and it doesn't take many 10 ms glitches
to start adding up to `real money
<https://google.com/search?q=latency+cost>`__; async disk I/O can help
prevent those. Just don't expect it to be magic, and be aware of the
tradeoffs.


.. _async-file-io-overview:

API overview
~~~~~~~~~~~~

If you want to perform general filesystem operations like creating and
listing directories, renaming files, or checking file metadata – or if
you just want a friendly way to work with filesystem paths – then you
want :class:`trio.Path`. It's an asyncified replacement for the
standard library's :class:`pathlib.Path`, and provides the same
comprehensive set of operations.

For reading and writing to files and file-like objects, Trio also
provides a mechanism for wrapping any synchronous file-like object
into an asynchronous interface. If you have a :class:`trio.Path`
object you can get one of these by calling its :meth:`~trio.Path.open`
method; or if you know the file's name you can open it directly with
:func:`trio.open_file`. Alternatively, if you already have an open
file-like object, you can wrap it with :func:`trio.wrap_file` – one
case where this is especially useful is to wrap :class:`io.BytesIO` or
:class:`io.StringIO` when writing tests.


Asynchronous path objects
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Path
   :members:


.. _async-file-objects:

Asynchronous file objects
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: open_file

.. autofunction:: wrap_file

.. interface:: Asynchronous file interface

   Trio's asynchronous file objects have an interface that
   automatically adapts to the object being wrapped. Intuitively, you
   can mostly treat them like a regular :term:`file object`, except
   adding an ``await`` in front of any of methods that do I/O. The
   definition of :term:`file object` is a little vague in Python
   though, so here are the details:

   * Synchronous attributes/methods: if any of the following
     attributes or methods are present, then they're re-exported
     unchanged: ``closed``, ``encoding``, ``errors``, ``fileno``,
     ``isatty``, ``newlines``, ``readable``, ``seekable``,
     ``writable``, ``buffer``, ``raw``, ``line_buffering``,
     ``closefd``, ``name``, ``mode``, ``getvalue``, ``getbuffer``.

   * Async methods: if any of the following methods are present, then
     they're re-exported as an async method: ``flush``, ``read``,
     ``read1``, ``readall``, ``readinto``, ``readline``,
     ``readlines``, ``seek``, ``tell``, ``truncate``, ``write``,
     ``writelines``, ``readinto1``, ``peek``, ``detach``.

   Special notes:

   * Async file objects implement trio's
     :class:`~trio.abc.AsyncResource` interface: you close them by
     calling :meth:`~trio.abc.AsyncResource.aclose` instead of
     ``close`` (!!), and they can be used as async context
     managers. Like all :meth:`~trio.abc.AsyncResource.aclose`
     methods, the ``aclose`` method on async file objects is
     guaranteed to close the file before returning, even if it is
     cancelled or otherwise raises an error.

   * Using the same async file object from multiple tasks
     simultaneously: because the async methods on async file objects
     are implemented using threads, it's only safe to call two of them
     at the same time from different tasks IF the underlying
     synchronous file object is thread-safe. You should consult the
     documentation for the object you're wrapping. For objects
     returned from :func:`trio.open_file` or :meth:`trio.Path.open`,
     it depends on whether you open the file in binary mode or text
     mode: `binary mode files are task-safe/thread-safe, text mode
     files are not
     <https://docs.python.org/3/library/io.html#multi-threading>`__.

   * Async file objects can be used as async iterators to iterate over
     the lines of the file::

        async with await trio.open_file(...) as f:
            async for line in f:
                print(line)

   * The ``detach`` method, if present, returns an async file object.

   This should include all the attributes exposed by classes in
   :mod:`io`. But if you're wrapping an object that has other
   attributes that aren't on the list above, then you can access them
   via the ``.wrapped`` attribute:

   .. attribute:: wrapped

      The underlying synchronous file object.


.. _subprocess:

Spawning subprocesses
---------------------

Trio provides support for spawning other programs as subprocesses,
communicating with them via pipes, sending them signals, and waiting
for them to exit. The interface for doing so consists of two layers.
The :class:`~trio.Process` class provides an interface that mirrors
the standard :class:`subprocess.Popen`, with async methods and with
Trio stream wrappers for the process's input and output. Built atop
:class:`~trio.Process` are the convenience functions
:func:`~trio.run_process`, :func:`~trio.delegate_to_process`, and
:func:`~trio.interact_with_process`.

In all of these interfaces, the :ref:`command to run and its arguments
<subprocess-quoting>` are specified as the sole positional argument,
while keyword :ref:`options <subprocess-options>` determine how the
subprocess's inputs and outputs will be set up and other aspects of
the environment in which it will run.  Almost all of the keyword
arguments accepted by the standard library :class:`subprocess.Popen`
class are supported with their usual semantics, and can be passed
wherever you see ``**options`` in the API documentation.

Trio's subprocess API makes use of many of the constants and exceptions
defined by the :mod:`subprocess` module in the standard library.
It reexports these, minus the non-async-friendly code, as the
submodule ``trio.subprocess``. So, if you'd like, you can say
``from trio import subprocess`` and then refer to ``subprocess.PIPE``,
:exc:`subprocess.CalledProcessError`, and so forth, without worrying
about errant calls to synchronous functions like :func:`subprocess.run`.
``trio.subprocess`` contains nothing but standard :mod:`subprocess`
reexports; the actual API is all in the top-level ``trio`` package.


.. _subprocess-quoting:

Specifying the command to run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The command to run and its arguments may be passed to any of Trio's
subprocess APIs as either a sequence of strings or a string.

* If a sequence is passed, the first element in the sequence specifies
  the command to run and the remaining elements specify its arguments,
  one argument per element. This form is recommended because it
  avoids potential quoting pitfalls; for example, you can run
  ``["cp", "-f", source_file, dest_file]`` without worrying about
  whether ``source_file`` or ``dest_file`` contains spaces.

* If a string is passed, it is split into words following POSIX
  shell quoting rules (the same used by :func:`shlex.split`),
  even on Windows. These can be summarized as:

  * Outside of quotes, an unescaped backslash escapes the following
    character (removes its special meaning, if any) and does not
    appear in the output.

  * Outside of quotes, any contiguous run of unescaped whitespace
    acts as a separator between words.

  * Inside double quotes, no word splitting occurs; backslash may
    be used to escape a backslash or double quote, but will be passed
    through as a normal character before anything else.

  * Inside single quotes, no word splitting occurs and no escape
    sequences are recognized; it is not possible to write a single
    quote within single quotes.

* If a string is passed and the ``local_quotes=True`` option is given,
  it will be assumed to follow Windows quoting rules (no special
  meaning for single quotes, no special meaning for backslashes except
  immediately before a double quote) when running on Windows platforms,
  and POSIX rules when running on UNIX platforms.

Trio performs this normalization because the underlying system
subprocess-spawning functions vary in what sort of input they
want: a sequence on UNIX platforms with ``shell=False``, a POSIX-quoted
string on UNIX platforms with ``shell=True``, and a Windows-quoted
string on Windows platforms regardless of the value of ``shell``.
The Python standard library doesn't provide any facility for turning
a Windows-quoted string into a sequence of arguments, so it's easier
to standardize on POSIX quotes.

.. list-table:: Summary of quoting behavior
   :widths: auto
   :header-rows: 1

   * - Platform
     - ``shell=``
     - Sequence behavior
     - String behavior
     - ... with ``local_quotes=True``
   * - UNIX
     - False
     - Use as-is
     - :func:`shlex.split`
     - :func:`shlex.split`
   * - UNIX
     - True
     - :func:`shlex.quote`
     - Use as-is
     - Use as-is
   * - Windows
     - False
     - Windows-quote
     - :func:`shlex.split` then Windows-quote
     - Use as-is
   * - Windows
     - True
     - Windows-quote
     - :func:`shlex.split` then Windows-quote
     - Use as-is


.. _subprocess-options:

Options for starting subprocesses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standard :mod:`subprocess` module supports a dizzying array
of `options <https://docs.python.org/3/library/subprocess.html#popen-constructor>`__
for controlling the environment in which a process starts and the
mechanisms used for communicating with it. (If you find that list
overwhelming, you're not alone; you might prefer to start with
just the `frequently used ones
<https://docs.python.org/3/library/subprocess.html#frequently-used-arguments>`__.)

Trio makes use of the :mod:`subprocess` module's logic for spawning processes,
so almost all of these options can be used with their same semantics when
starting subprocesses under Trio. The exceptions are ``encoding``, ``errors``,
``universal_newlines`` (and its 3.7+ alias ``text``), and ``bufsize``;
Trio always uses unbuffered byte streams for communicating with a process,
so these options don't make sense. Text I/O should use a layer
on top of the raw byte streams, just as it does with sockets.
[This layer does not yet exist, but is in the works.]

The Trio-specific boolean option ``local_quotes``, described
:ref:`above <subprocess-quoting>`, is also supported in all
``**options`` lists.


Running a process and waiting for it to finish
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The basic interface for running a subprocess start-to-finish is
:func:`trio.run_process`.  It always waits for the subprocess to exit
before returning, so there's no need to worry about leaving a process
running by mistake after you've gone on to do other things.
:func:`~trio.run_process` is similar to the standard library
:func:`subprocess.run` function, but tries to have safer defaults:
with no options, the subprocess's input is provided and its outputs
are captured by the parent Trio process rather than connecting them
to the user's terminal, and a failure in the subprocess will be propagated
as a :exc:`subprocess.CalledProcessError` exception. Of course, these
defaults can be changed where necessary.

.. autofunction:: trio.run_process

If a subprocess's input and output are to come from an interactive user
-- on the console used to start the parent Trio process, for example --
:func:`~trio.run_process`'s defaults can be slightly cumbersome. Trio
provides :func:`~trio.delegate_to_process` to cover this case.

.. autofunction:: trio.delegate_to_process


Interacting with a process as it runs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`interact_with_process` spawns a subprocess and returns an async
context manager in which the caller can interact with the subprocess's
standard input and output using a Trio stream. The lifetime of the
process is scoped within the ``async with`` block: the flow of execution
won't exit the block until the subprocess exits, and a cancellation of
the block will kill the subprocess. The effect is much like
:func:`run_process`, but with user code inserted in the middle.

.. autofunction:: trio.interact_with_process
   :async-with: stream

The specific :class:`~trio.abc.HalfCloseableStream` type provided by
:func:`~trio.interact_with_process` is also available for users that
wish to wrap a :class:`~trio.Process` object directly:

.. autoclass:: trio.ProcessStream
   :show-inheritance:
   :members:


The low-level Process API
~~~~~~~~~~~~~~~~~~~~~~~~~

All of the high-level subprocess functions described above are implemented
in terms of the :class:`trio.Process` class, which provides an interface
similar to the standard library's :class:`subprocess.Popen`.

.. autoclass:: trio.Process

   .. autoattribute:: returncode

   .. autoattribute:: failed

   .. automethod:: aclose

   .. automethod:: wait

   .. automethod:: poll

   .. automethod:: kill

   .. automethod:: terminate

   .. automethod:: send_signal

   .. note:: :meth:`~subprocess.Popen.communicate` is not provided as a
      method on :class:`~trio.Process` objects; use a higher-level
      function instead, or write the loop yourself if you have unusual
      needs. :meth:`~subprocess.Popen.communicate` has quite unusual
      cancellation behavior in the standard library (on some platforms it
      spawns a background thread which continues to read from the child
      process even after the timeout has expired) and we wanted to
      provide an interface with fewer surprises.


Signals
-------

.. currentmodule:: trio

.. autofunction:: open_signal_receiver
   :with: signal_aiter
