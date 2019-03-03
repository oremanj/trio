import attr
import collections
import inspect
import outcome
import types
import weakref
from contextlib import closing, ExitStack, contextmanager
try:
    from contextlib import AsyncExitStack
except ImportError:
    from async_exit_stack import AsyncExitStack
from functools import wraps, update_wrapper, partial
from .. import _core
from ._ki import LOCALS_KEY_KI_PROTECTION_ENABLED, enable_ki_protection


__all__ = ["atomic_operation", "select"]


class SpuriousWakeup(BaseException):
    """Internal exception thrown to implement operation retry."""


def raise_through_opiter(opiter, exc, *, may_swallow=False):
    """Raise ``exc``, by throwing it into ``opiter`` with the expectation
    that it will propagate out. This serves to include in the traceback
    of ``exc`` the location at which ``opiter`` was suspended, and
    is appropriate for exceptions relating to misbehavior of the
    operation function.

    Args:
      opiter: The operation iterator (or any generator iterator) to
          throw the exception into.
      exc: The exception to throw.
      may_swallow: If True, ``opiter`` is allowed to respond to the
          exception by returning normally (raising :exc:`StopIteration`).
          Otherwise, such a response will be converted into a
          :exc:`RuntimeError`.

    Raises:
      ``exc``, or another exception that ``opiter`` raised in its place,
      or :exc:`RuntimeError` if ``opiter`` responded to ``exc`` by yielding
      again or (if ``may_swallow`` is its default of False) by
      returning normally.

    """
    try:
        opiter.throw(type(exc), exc, exc.__traceback__)
    except (() if may_swallow else StopIteration):
        raise RuntimeError("operation function swallowed exception") from exc
    try:
        raise RuntimeError("operation function ignored exception") from exc
    finally:
        opiter.close()


@attr.s(repr=False, cmp=False, slots=True)
class CompletionHandle:
    """A representation of the ability to complete an operation.

    Each individual operation gets its own ...
    """

    _opiter = attr.ib()
    _parent_handle = attr.ib()
    _close_children_fn = attr.ib()
    custom_sleep_data = attr.ib(default=None)

    def __str__(self):
        return (
            str(self._parent_handle) + " -> " + self._opiter.gi_code.co_name
        )

    def __repr__(self):
        return "<operation completion handle at {:#x}, for {}>".format(
            id(self), str(self)
        )

    @enable_ki_protection
    def complete(self, inner_value=None):
        try:
            self._close_children_fn()
        except BaseException as ex:
            self.fail(ex)
            return

        try:
            self._opiter.send(inner_value)
            raise_through_opiter(
                self._opiter,
                RuntimeError("operation function yielded too many times"),
            )
        except StopIteration as outer_result:
            self._parent_handle.complete(outer_result.value)
        except BaseException as outer_error:
            self._parent_handle.fail(outer_error)

    @enable_ki_protection
    def fail(self, inner_error):
        if isinstance(inner_error, type):
            try:
                inner_error = inner_error()
            except BaseException as ex:
                inner_error = TypeError(
                    "operation {} completion fail() was called with an "
                    "uninstantiable exception type {!r}"
                    .format(str(self), inner_error)
                )
                inner_error.__cause__ = ex
        if not isinstance(inner_error, BaseException):
            inner_error = TypeError(
                "operation {} completion fail() was called with "
                "{!r} which is not an exception type or value"
                .format(str(self), inner_error)
            )

        try:
            self._close_children_fn()
        except BaseException as ex:
            ex.__context__ = inner_error
            inner_error = ex

        try:
            raise_through_opiter(self._opiter, inner_error, may_swallow=True)
        except StopIteration as outer_result:
            self._parent_handle.complete(outer_result.value)
        except BaseException as outer_error:
            self._parent_handle.fail(outer_error)

    def retry(self):
        raise SpuriousWakeup

    def add_async_cleanup(self, async_fn, *args):
        self._parent_handle.add_async_cleanup(async_fn, *args)


@attr.s
class ClosingStack:
    _exit_stack = attr.ib(factory=ExitStack)
    _items = attr.ib(factory=list)

    def push(self, item):
        self._exit_stack.callback(item.close)
        self._items.append(item)
        return item

    def close(self):
        del self._items[:]
        self._exit_stack.close()

    def close_from(self, index):
        with ExitStack() as stack:
            for item in self._items[index:]:
                stack.callback(item.close)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self._exit_stack.__exit__(*exc)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, index):
        return self._items[index]


@attr.s(slots=True, repr=False)
class Operation(collections.abc.Coroutine):
    func = attr.ib()
    args = attr.ib()
    keywords = attr.ib()
    _first_opiter = attr.ib(default=None, init=False, cmp=False)
    _coro = attr.ib(default=None, init=False, cmp=False)

    def __attrs_post_init__(self):
        self._first_opiter = self._get_opiter()
        self._coro = perform_operation(self)
        if hasattr(self.func, "__qualname__"):
            self._coro.__qualname__ = self.func.__qualname__
            self._coro.__name__ = self.func.__name__
        else:
            self._coro.__qualname__ = repr(self.func)
            self._coro.__name__ = repr(self.func)

    @property
    def name(self):
        return self._coro.__qualname__

    def __str__(self):
        def fmt(arg):
            return str(arg) if isinstance(arg, Operation) else repr(arg)

        args = list(map(fmt, self.args)) + [
            "{}={}".format(key, fmt(value))
            for key, value in self.keywords.items()
        ]
        return "{}({})".format(self.name, ", ".join(args))

    def __repr__(self):
        return "<operation {}>".format(str(self))

    # It's a coroutine!

    def send(self, value):
        return self._coro.send(value)

    def throw(self, *exc):
        return self._coro.throw(*exc)

    def close(self):
        self._coro.close()

    def __await__(self):
        return self._coro.__await__()

    @property
    def cr_await(self):
        return self._coro.cr_await

    @property
    def cr_code(self):
        return self._coro.cr_code

    @property
    def cr_frame(self):
        return self._coro.cr_frame

    @property
    def cr_running(self):
        return self._coro.cr_running

    def _discard_coro(self, why):
        state = inspect.getcoroutinestate(self._coro)
        if state == inspect.CORO_CREATED:
            self._coro.close()
        elif state != inspect.CORO_CLOSED:
            raise RuntimeError(
                "can't use {!r} with {} if you've already awaited it"
                .format(self, why)
            )

    # It's an iterable!

    def _get_opiter(self):
        if self._first_opiter is not None:
            opiter = self._first_opiter
            self._first_opiter = None
            return opiter
        opiter = self.func(*self.args, **self.keywords)
        if not inspect.isgenerator(opiter):
            raise TypeError(
                "{!r} must return a generator iterator, not {!r}"
                .format(self.name, type(opiter))
            )
        opiter.gi_frame.f_locals.setdefault(
            LOCALS_KEY_KI_PROTECTION_ENABLED, True
        )
        return opiter

    def __iter__(self):
        self._discard_coro("operation composition")
        return self._get_opiter()

    # It's Superman!

    def perform(self):
        self._discard_coro("perform()")
        return perform_operation(self)

    def attempt(self):
        self._discard_coro("attempt()")
        with ClosingStack() as opiter_stack:
            return attempt_operation(self._get_opiter(), opiter_stack)


def attempt_operation(opiter, opiter_stack):
    try:
        first_yield = opiter.send(None)
    except StopIteration as ex:
        return ex.value

    opiter_stack.push(opiter)
    if first_yield is None:
        raise _core.WouldBlock
    if not isinstance(first_yield, Operation):
        raise_through_opiter(
            opiter,
            RuntimeError(
                "operation function yielded {!r} at its first yield point; "
                "expected None or another operation to delegate to"
                .format(first_yield)
            ),
        )
    return attempt_operation(iter(first_yield), opiter_stack)


def publish_operation(handle, opiter_stack):
    for idx, opiter in enumerate(opiter_stack):
        handle = CompletionHandle(
            opiter, handle, partial(opiter_stack.close_from, idx + 1)
        )
        try:
            second_yield = opiter.send(handle)
        except StopIteration as ex:
            raise TypeError(
                "operation function yielded too few times"
            ) from ex
        if second_yield is not None:
            raise_through_opiter(
                opiter,
                RuntimeError(
                    "operation function yielded {!r} at its second yield "
                    "point; expected None".format(second_yield)
                ),
            )


@attr.s(cmp=False, slots=True)
class RootHandle:
    sleeping_task = attr.ib(default=None)
    completed = attr.ib(default=False)
    async_cleanup_stack = attr.ib(factory=AsyncExitStack)

    def __str__(self):
        return self.sleeping_task.name

    @enable_ki_protection
    def reschedule(self, operation_outcome):
        if self.completed:
            raise RuntimeError("that operation has already been completed")
        if self.sleeping_task is None:
            raise RuntimeError(
                "can't complete an operation from within its own operation "
                "function"
            )
        _core.reschedule(self.sleeping_task, operation_outcome)
        self.completed = True
        self.sleeping_task = None

    @enable_ki_protection
    def complete(self, value):
        self.reschedule(outcome.Value(value))

    @enable_ki_protection
    def fail(self, error):
        self.reschedule(outcome.Error(error))

    def add_async_cleanup(self, async_fn, *args):
        if self.sleeping_task is None or self.completed:
            raise RuntimeError(
                "add_async_cleanup() must be called after the second "
                "yield point in an operation function"
            )
        self.async_cleanup_stack.push_async_callback(async_fn, *args)


async def perform_operation(operation):
    did_block = False
    await _core.checkpoint_if_cancelled()
    try:
        while True:
            with ClosingStack() as opiter_stack:
                try:
                    return attempt_operation(
                        operation._get_opiter(), opiter_stack
                    )
                except _core.WouldBlock:
                    pass

                root_handle = RootHandle()
                publish_operation(root_handle, opiter_stack)

                def abort_fn(raise_cancel):
                    try:
                        opiter_stack.close()
                    except BaseException as ex:
                        root_handle.fail(ex)
                        return _core.Abort.FAILED
                    else:
                        root_handle.completed = True
                        root_handle.sleeping_task = None
                        return _core.Abort.SUCCEEDED

                root_handle.sleeping_task = _core.current_task()
                did_block = True
                try:
                    return await _core.wait_task_rescheduled(abort_fn)
                except SpuriousWakeup:
                    pass
                finally:
                    if root_handle.async_cleanup_stack:
                        with _core.CancelScope(shield=True):
                            await root_handle.async_cleanup_stack.aclose()
    finally:
        if not did_block:
            await _core.cancel_shielded_checkpoint()


def atomic_operation(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return Operation(fn, args, kwargs)
    return wrapper


@atomic_operation
def select(*operations):
    with ClosingStack() as toplevel_stack:
        opiters = map(iter, operations)

        for opiter in opiters:
            branch_stack = toplevel_stack.push(ClosingStack())
            try:
                return attempt_operation(opiter, branch_stack)
            except _core.WouldBlock:
                pass

        handle = yield

        for branch_stack in toplevel_stack:
            publish_operation(handle, branch_stack)

        return (yield)
