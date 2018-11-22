import errno
import select
import random

import os
import pytest

from trio._core.tests.tutil import gc_collect_harder
from ... import _core, move_on_after, sleep, BrokenResourceError
from ...testing import (wait_all_tasks_blocked, check_one_way_stream)

posix = os.name == "posix"

pytestmark = pytest.mark.skipif(
    not posix, reason="pipes are only supported on posix"
)

if posix:
    from ..._subprocess.unix_pipes import (
        PipeSendStream, PipeReceiveStream, make_pipe
    )


async def test_send_pipe():
    r, w = os.pipe()
    send = PipeSendStream(w)
    assert send.fileno() == w
    await send.send_all(b"123")
    assert (os.read(r, 8)) == b"123"

    os.close(r)
    os.close(w)
    send._closed = True


async def test_receive_pipe():
    r, w = os.pipe()
    recv = PipeReceiveStream(r)
    assert (recv.fileno()) == r
    os.write(w, b"123")
    assert (await recv.receive_some(8)) == b"123"

    os.close(r)
    os.close(w)
    recv._closed = True


async def test_pipes_combined():
    write, read = await make_pipe()
    count = 2**20

    async def sender():
        big = bytearray(count)
        await write.send_all(big)

    async def reader():
        await wait_all_tasks_blocked()
        received = 0
        while received < count:
            received += len(await read.receive_some(4096))

        assert received == count

    async with _core.open_nursery() as n:
        n.start_soon(sender)
        n.start_soon(reader)

    await read.aclose()
    await write.aclose()


async def test_send_on_closed_pipe():
    write, read = await make_pipe()
    await write.aclose()

    with pytest.raises(_core.ClosedResourceError):
        await write.send_all(b"123")

    await read.aclose()


async def test_pipe_errors():
    with pytest.raises(TypeError):
        PipeReceiveStream(None)

    with pytest.raises(ValueError):
        await PipeReceiveStream(0).receive_some(0)


async def test_del():
    w, r = await make_pipe()
    f1, f2 = w.fileno(), r.fileno()
    del w, r
    gc_collect_harder()

    with pytest.raises(OSError) as excinfo:
        os.close(f1)
    assert excinfo.value.errno == errno.EBADF

    with pytest.raises(OSError) as excinfo:
        os.close(f2)
    assert excinfo.value.errno == errno.EBADF


async def test_async_with():
    w, r = await make_pipe()
    async with w, r:
        pass

    assert w._closed
    assert r._closed

    with pytest.raises(OSError) as excinfo:
        os.close(w.fileno())
    assert excinfo.value.errno == errno.EBADF

    with pytest.raises(OSError) as excinfo:
        os.close(r.fileno())
    assert excinfo.value.errno == errno.EBADF


async def make_clogged_pipe():
    s, r = await make_pipe()
    try:
        while True:
            # We want to totally fill up the pipe buffer.
            # This requires working around a weird feature that POSIX pipes
            # have.
            # If you do a write of <= PIPE_BUF bytes, then it's guaranteed
            # to either complete entirely, or not at all. So if we tried to
            # write PIPE_BUF bytes, and the buffer's free space is only
            # PIPE_BUF/2, then the write will raise BlockingIOError... even
            # though a smaller write could still succeed! To avoid this,
            # make sure to write >PIPE_BUF bytes each time, which disables
            # the special behavior.
            # For details, search for PIPE_BUF here:
            #   http://pubs.opengroup.org/onlinepubs/9699919799/functions/write.html

            # for the getattr:
            # https://bitbucket.org/pypy/pypy/issues/2876/selectpipe_buf-is-missing-on-pypy3
            buf_size = getattr(select, "PIPE_BUF", 8192)
            os.write(s.fileno(), b"x" * buf_size * 2)
    except BlockingIOError:
        pass
    return s, r


async def test_pipe_fully():
    await check_one_way_stream(make_pipe, make_clogged_pipe)


async def test_pipe_send_some(autojump_clock):
    write, read = await make_pipe()
    data = bytearray(random.randint(0, 255) for _ in range(2**18))
    next_send_offset = 0
    received = bytearray()

    async def sender():
        nonlocal next_send_offset
        with move_on_after(2.0):
            while next_send_offset < len(data):
                next_send_offset = await write.send_some(
                    data, next_send_offset
                )
        await write.aclose()

    async def reader():
        nonlocal received
        await wait_all_tasks_blocked()
        while True:
            await sleep(0.1)
            chunk = await read.receive_some(4096)
            if chunk == b"":
                break
            received.extend(chunk)

    async with _core.open_nursery() as n:
        n.start_soon(sender)
        n.start_soon(reader)

    assert received == data[:next_send_offset]

    await read.aclose()

    write, read = await make_pipe()
    await read.aclose()
    with pytest.raises(BrokenResourceError):
        await write.send_some(data, next_send_offset)
    await write.aclose()
