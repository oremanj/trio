import attr
import outcome
from async_generator import async_generator, asynccontextmanager, yield_

import pytest

from ... import _core
from ...testing import assert_checkpoints, wait_all_tasks_blocked


@attr.s
class Record:
    trace = attr.ib(factory=list)  # [(ident, step)]
    handle = attr.ib(factory=dict)  # {ident: completion handle}
    result = attr.ib(factory=dict)  # {ident: result}
    final_outcome = attr.ib(init=False)


def context_chain(exc):
    while exc is not None:
        yield type(exc)
        exc = exc.__cause__ or exc.__context__


@_core.atomic_operation
def tracing_op(record, ident, next_=None, immediate_result=None):
    record.trace.append((ident, "attempt"))
    if immediate_result is not None:
        return immediate_result + " " + ident
    try:
        record.handle[ident] = yield next_
    except BaseException as ex:
        record.result[ident] = ex
        record.trace.append((ident, "drop"))
        raise
    record.trace.append((ident, "publish"))
    try:
        record.result[ident] = yield
        record.trace.append((ident, "unpublish"))
        return record.result[ident] + " " + ident
    except BaseException as ex:
        record.result[ident] = ex
        record.trace.append((ident, "unpublish (exc)"))
        raise


@asynccontextmanager
@async_generator
async def published(record, op):
    async def run_op(task_status):
        # There are no checkpoints in the publish part of perform_operation,
        # so calling started() beforehand is good enough.
        task_status.started()
        record.final_outcome = await outcome.acapture(op.perform)

    async with _core.open_nursery() as nursery:
        await nursery.start(run_op, name="run_op")
        await yield_()


async def test_smoke():
    record = Record()
    op = tracing_op(record, "outer", tracing_op(record, "inner"))
    assert repr(op).startswith("<operation tracing_op(Record(")
    assert record.trace == []

    with pytest.raises(_core.WouldBlock):
        op.attempt()
    assert record.trace == [
        ("outer", "attempt"),
        ("inner", "attempt"),
        ("inner", "drop"),
        ("outer", "drop"),
    ]
    assert not record.handle
    assert type(record.result["outer"]) is GeneratorExit
    assert type(record.result["inner"]) is GeneratorExit
    del record.trace[:]
    record.result.clear()

    async with published(record, op):
        assert record.trace == [
            ("outer", "attempt"),
            ("inner", "attempt"),
            ("outer", "publish"),
            ("inner", "publish"),
        ]
        assert repr(record.handle["inner"]).startswith(
            "<operation completion handle at "
        )
        assert repr(record.handle["inner"]).endswith(
            "for run_op -> tracing_op -> tracing_op>"
        )
        assert repr(record.handle["outer"]).endswith(
            "for run_op -> tracing_op>"
        )
        assert record.handle["outer"] is not record.handle["inner"]
        assert not record.result
        record.handle["inner"].complete("done")
        with pytest.raises(RuntimeError) as exc_info:
            record.handle["inner"].complete("whoops")
        assert "operation has already been completed" in str(exc_info.value)
    assert record.trace[4:] == [
        ("inner", "unpublish"),
        ("outer", "unpublish"),
    ]
    assert record.result == {
        "inner": "done",
        "outer": "done inner",
    }
    assert record.final_outcome == outcome.Value("done inner outer")


async def test_throws_during_close_children():
    @_core.atomic_operation
    def throws_when_closed(typ1, typ2, next_=None):
        try:
            yield next_
        except:
            record.trace.append(typ1)
            raise typ1
        try:
            yield
        except:
            record.trace.append(typ2)
            raise typ2

    record = Record()
    op = tracing_op(
        record, "op", throws_when_closed(KeyError, ValueError,
                                         throws_when_closed(IndexError, RuntimeError))
    )

    with pytest.raises(KeyError) as info:
        op.attempt()
    assert record.trace == [
        ("op", "attempt"),
        IndexError,
        KeyError,
        ("op", "drop"),
    ]
    del record.trace[:]

    async with published(record, op):
        record.handle["op"].complete("")
    assert record.trace == [
        ("op", "attempt"),
        ("op", "publish"),
        RuntimeError,
        ValueError,
        ("op", "unpublish (exc)"),
    ]
    del record.trace[:]
    assert list(context_chain(record.final_outcome.error)) == [
        ValueError, GeneratorExit, RuntimeError, GeneratorExit
    ]

    async with published(record, op):
        record.handle["op"].fail(OSError)
    assert record.trace == [
        ("op", "attempt"),
        ("op", "publish"),
        RuntimeError,
        ValueError,
        ("op", "unpublish (exc)"),
    ]
    del record.trace[:]
    assert list(context_chain(record.final_outcome.error)) == [
        OSError, ValueError, GeneratorExit, RuntimeError, GeneratorExit
    ]


async def test_fail_with_bad_argument():
    record = Record()
    async with published(record, tracing_op(record, "op")):
        record.handle["op"].fail(int)
    assert type(record.final_outcome.error) is TypeError
    assert "fail() was called with 0 which is not an exception" in str(
        record.final_outcome.error
    )
    assert record.trace == [
        ("op", "attempt"), ("op", "publish"), ("op", "unpublish (exc)")
    ]
    del record.trace[:]

    async with published(record, tracing_op(record, "op")):
        record.handle["op"].fail(type)
    assert type(record.final_outcome.error) is TypeError
    assert "fail() was called with an uninstantiable exception type" in str(
        record.final_outcome.error
    )
    assert type(record.final_outcome.error.__cause__) is TypeError
    assert "takes 1 or 3 arg" in str(record.final_outcome.error.__cause__)
    assert record.trace == [
        ("op", "attempt"), ("op", "publish"), ("op", "unpublish (exc)")
    ]
    del record.trace[:]


async def test_protocol_violations():
    @_core.atomic_operation
    def too_many_yields(next_):
        yield next_
        yield
        yield

    record = Record()
    async with published(
        record, too_many_yields(tracing_op(record, "op"))
    ):
        record.handle["op"].complete("")
    assert list(context_chain(record.final_outcome.error)) == [RuntimeError]
    assert "yielded too many times" in str(record.final_outcome.error)
    tb_bottom = record.final_outcome.error.__traceback__
    while tb_bottom.tb_next is not None:
        tb_bottom = tb_bottom.tb_next
    assert tb_bottom.tb_frame.f_code.co_name == "too_many_yields"

    @_core.atomic_operation
    def too_many_yields_and_swallow(next_):
        yield next_
        yield
        try:
            yield
        except:
            pass

    async with published(
        record, too_many_yields_and_swallow(tracing_op(record, "op"))
    ):
        record.handle["op"].complete("")
    assert list(context_chain(record.final_outcome.error)) == [
        RuntimeError, RuntimeError
    ]
    assert "operation function swallowed exception" in str(
        record.final_outcome.error
    )
    assert "yielded too many times" in str(
        record.final_outcome.error.__cause__
    )

    @_core.atomic_operation
    def too_many_yields_and_raise(next_):
        yield next_
        yield
        try:
            yield
        except:
            raise KeyError

    async with published(
        record, too_many_yields_and_raise(tracing_op(record, "op"))
    ):
        record.handle["op"].complete("")
    assert list(context_chain(record.final_outcome.error)) == [
        KeyError, RuntimeError
    ]
    assert "yielded too many times" in str(
        record.final_outcome.error.__context__
    )

    @_core.atomic_operation
    def too_many_yields_and_yield(next_):
        yield next_
        yield
        try:
            yield
        finally:
            yield

    async with published(
        record, too_many_yields_and_yield(tracing_op(record, "op"))
    ):
        record.handle["op"].complete("")
    assert list(context_chain(record.final_outcome.error)) == [
        RuntimeError, RuntimeError
    ]
    assert "operation function ignored exception" in str(
        record.final_outcome.error
    )
    assert "yielded too many times" in str(
        record.final_outcome.error.__cause__
    )

    @_core.atomic_operation
    def invalid_first_yield():
        yield 42
    with pytest.raises(RuntimeError) as exc_info:
        invalid_first_yield().attempt()
    assert "expected None or another operation" in str(exc_info.value)
    tb_bottom = exc_info.value.__traceback__
    while tb_bottom.tb_next is not None:
        tb_bottom = tb_bottom.tb_next
    assert tb_bottom.tb_frame.f_code.co_name == "invalid_first_yield"

    @_core.atomic_operation
    def too_few_yields():
        yield
        return 100
    with pytest.raises(RuntimeError) as exc_info:
        await too_few_yields()
    assert "yielded too few times" in str(exc_info.value)

    @_core.atomic_operation
    def invalid_second_yield():
        yield
        yield 100
    with pytest.raises(RuntimeError) as exc_info:
        await invalid_second_yield()
    assert "yielded 100 at its second yield point" in str(exc_info.value)
    tb_bottom = exc_info.value.__traceback__
    while tb_bottom.tb_next is not None:
        tb_bottom = tb_bottom.tb_next
    assert tb_bottom.tb_frame.f_code.co_name == "invalid_second_yield"

    @_core.atomic_operation
    def invalid_handle_ops(next_):
        handle = yield next_
        with pytest.raises(RuntimeError, match=r"can't complete.*its own.*"):
            handle.complete()
        with pytest.raises(RuntimeError, match=r".*called after the second.*"):
            handle.retry()
        with pytest.raises(RuntimeError, match=r".*called after the second.*"):
            handle.add_async_cleanup(_core.checkpoint)
        yield
        with pytest.raises(RuntimeError, match=r"can't complete.*its own.*"):
            handle.complete()
    async with published(
        record, invalid_handle_ops(tracing_op(record, "op"))
    ):
        record.handle["op"].complete("x")
    assert record.final_outcome.value is None


async def test_failure_suppressed():
    @_core.atomic_operation
    def suppress_it(next_):
        yield next_
        try:
            yield
        except OSError:
            return 42

    record = Record()
    async with published(record, suppress_it(tracing_op(record, "op"))):
        record.handle["op"].fail(OSError("wat"))
    assert record.final_outcome == outcome.Value(42)


async def test_callable_without_qualname():
    class Foo:
        def __repr__(self):
            return "<Foo>"
        def __call__(self):
            yield  # pragma: no cover

    op = _core.atomic_operation(Foo())()
    assert op.name == "<Foo>"
    op.close()


async def test_coroutine_interface():
    @_core.atomic_operation
    def dummy(handle_holder=None):
        if handle_holder is None:
            return
        handle_holder[0] = yield
        yield

    with pytest.raises(RuntimeError):
        dummy().throw(RuntimeError)
    holder = [None]
    op = dummy(holder)
    assert op.cr_await is None
    assert op.cr_code is _core._operation.perform_operation.__code__
    assert op.cr_frame is not None
    assert op.cr_running is False
    async with _core.open_nursery() as nursery:
        with pytest.raises(TypeError):
            nursery.start_soon(op)
        nursery.start_soon(dummy)
        nursery.start_soon(lambda: op)
        await wait_all_tasks_blocked()
        with pytest.raises(RuntimeError) as exc_info:
            op.attempt()
        assert "if you've already awaited it" in str(exc_info.value)
        holder[0].complete()


def test_usage_errors():
    @_core.atomic_operation
    def example():
        return 42

    # argument mismatch is detected immediately
    with pytest.raises(TypeError) as exc_info:
        example(10)
    assert "0 positional arguments but 1 was given" in str(exc_info.value)

    # so is not being a generator
    with pytest.raises(TypeError) as exc_info:
        example()
    assert "generator iterator, not <class 'int'>" in str(exc_info.value)


async def test_composition():
    # TODO
    pass


async def test_exception_during_abort():
    pass


async def test_exception_during_async_cleanup():
    pass


async def test_reversible():
    pass


