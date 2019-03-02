import attr
import inspect
import outcome
import types
import warnings
import weakref
from contextlib import closing, ExitStack, contextmanager
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
        raise TypeError("operation function swallowed exception") from exc
    try:
        opiter.close()
    finally:
        raise TypeError("operation function ignored exception") from exc


@attr.s(repr=False, cmp=False, slots=True)
class CompletionHandle:
    """A representation of the ability to complete an operation.

    Each individual operation gets its own ...
    """

    _opiter = attr.ib()
    _parent_handle = attr.ib()
    _close_children_fn = attr.ib()
    custom_sleep_data = attr.ib(default=None)

    def _name(self):
        return (
            self._parent_handle._name()
            + " -> " + self._opiter.gi_code.co_name
        )

    def __repr__(self):
        return "<operation completion handle at {:#x}, for {}>".format(
            id(self), self._name()
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
                TypeError("operation function yielded too many times"),
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
                    .format(self._name(), inner_error)
                )
                inner_error.__cause__ = ex
        if not isinstance(inner_error, BaseException):
            inner_error = TypeError(
                "operation {} completion fail() was called with "
                "{!r} which is not an exception type or value"
                .format(self._name(), inner_error)
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

    def add_async_cleanup(self, async_fn):
        self._parent_handle.add_async_cleanup(async_fn)


@attr.s(slots=True, repr=False)
class Operation:
    fn = attr.ib()
    args = attr.ib()
    kwargs = attr.ib()
    _first_opiter = attr.ib(default=None, init=False, cmp=False)

    def _name(self):
        for attr in ("__qualname__", "__name__"):
            try:
                return getattr(self.fn, attr)
            except AttributeError:
                pass
        else:
            return repr(self.fn)

    def __attrs_post_init__(self):
        self._first_opiter = self()

    def __repr__(self):
        args = list(map(repr, self.args)) + [
            "{}={!r}".format(*item) for item in self.kwargs.items()
        ]
        return "{}.operation({})".format(self._name(), ", ".join(args))

    def __call__(self):
        if self._first_opiter is not None:
            opiter = self._first_opiter
            self._first_opiter = None
            return opiter

        opiter = self.fn(*self.args, **self.kwargs)
        if not inspect.isgenerator(opiter):
            raise TypeError(
                "{} must return a generator iterator, not {!r}"
                .format(self._name(), type(opiter))
            )
        opiter.gi_frame.f_locals.setdefault(
            LOCALS_KEY_KI_PROTECTION_ENABLED, True
        )
        return opiter

    # make 'yield from foo.operation()' work
    __iter__ = __call__


def is_operation_function(fn):
    return (
        callable(fn)
        and callable(getattr(fn, "operation", None))
        and callable(getattr(fn, "nowait", None))
    )


def get_opiter(operation):
    # If 'something' is a @atomic_operation function, we detect:
    if isinstance(operation, Operation):
        # operation = something.operation(...)
        return operation()
    if is_operation_function(operation):
        # operation = something
        return get_opiter(operation.operation())
    if (
        isinstance(operation, partial)
        and is_operation_function(operation.func)
    ):
        # operation = partial(something, ...)
        return get_opiter(
            operation.func.operation(*operation.args, **operation.keywords)
        )
    if (
        isinstance(operation, tuple)
        and operation and is_operation_function(operation[0])
    ):
        # operation = (something, ...)
        return get_opiter(operation[0](*operation[1:]))
    if (
        inspect.iscoroutine(operation)
        and operation.cr_code is perform_operation.__code__
    ):
        # operation = something(...) -- don't permit this, since it
        # instills bad habits (calling async functions without
        # 'await'), but do give a good error
        operation = operation.cr_frame.f_locals["operation"]
        if operaption.kwargs:
            use = "{0}.operation(...) or partial({0}, ...), not {0}(...)"
        elif operation.args:
            use = "{0}.operation(...) or ({0}, ...), not {0}(...)"
        else:
            use = "{0}.operation() or {0}, not {0}()"
        raise TypeError(msg.format(operation._name))
    raise TypeError(
        "need an operation, not {!r}".format(type(operation).__qualname__)
    )


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


def attempt_operation(opiter, opiter_stack):
    try:
        sub_operation = opiter.send(None)
    except StopIteration as ex:
        return ex.value

    opiter_stack.push(opiter)
    if sub_operation is None:
        raise _core.WouldBlock

    try:
        sub_opiter = get_opiter(sub_operation)
    except Exception as ex:
        raise_through_opiter(opiter, ex)

    return attempt_operation(sub_opiter, opiter_stack)


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
                TypeError(
                    "operation function yielded non-None value {!r} "
                    "at its second yield point".format(second_yield)
                ),
            )


@attr.s(cmp=False, slots=True)
class RootHandle:
    sleeping_task = attr.ib(default=None)
    completed = attr.ib(default=False)
    async_cleanup_fns = attr.ib(factory=list)

    def _name(self):
        return repr(self.sleeping_task)

    @enable_ki_protection
    def reschedule(self, operation_outcome):
        if self.completed:
            raise RuntimeError("that operation has already been completed")
        if self.sleeping_task is None:
            raise TypeError(
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

    def add_async_cleanup(self, async_fn):
        if self.sleeping_task is None or self.completed:
            raise TypeError(
                "add_async_cleanup() must be called after the final "
                "yield point in an operation function"
            )
        self.async_cleanup_fns.append(async_fn)


# The fact that this argument is named "operation" is relied upon
# to generate a better error message in get_opiter().
async def perform_operation(operation):
    did_block = False
    await _core.checkpoint_if_cancelled()
    try:
        while True:
            with ClosingStack() as opiter_stack:
                try:
                    return attempt_operation(
                        get_opiter(operation), opiter_stack
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
                    if root_handle.async_cleanup_fns:
                        with _core.CancelScope(shield=True):
                            if len(root_handle.async_cleanup_fns) == 1:
                                await root_handle.async_cleanup_fns[0]()
                            else:
                                async with _core.open_nursery() as nursery:
                                    for afn in root_handle.async_cleanup_fns:
                                        nursery.start_soon(afn)
    finally:
        if not did_block:
            await _core.cancel_shielded_checkpoint()


@attr.s(slots=True, frozen=True, repr=False)
class BoundOperationMethod:
    __func__ = attr.ib()
    __self__ = attr.ib()
    nowait = attr.ib()
    operation = attr.ib()

    def __repr__(self):
        return repr(types.MethodType(self.__func__, self.__self__))

    def __call__(self, *args, **kwargs):
        return self.__func__(self.__self__, *args, **kwargs)


class atomic_operation:
    def __init__(self, fn):
        self.__isabstractmethod__ = getattr(fn, "__isabstractmethod__", False)

        if type(fn) is staticmethod:
            self._bind = self._bind_static
            fn = fn.__func__
        elif type(fn) is classmethod:
            self._bind = self._bind_class
            fn = fn.__func__
        elif type(fn) is types.FunctionType:
            self._bind = self._bind_instance
        elif not callable(fn):
            raise TypeError("@atomic_operation must decorate a callable")
        elif hasattr(fn, "__get__"):
            raise TypeError(
                "@atomic_operation doesn't support decorating a {!r} (need a "
                "function, classmethod, staticmethod, or non-descriptor "
                "callable)".format(type(fn))
            )

        self._fn = fn
        update_wrapper(self, self._fn, updated=())
        self._cache = {}

        @wraps(fn)
        def wrapper(*args, **kwargs):
            return perform_operation(Operation(fn, args, kwargs))

        @wraps(fn)
        def wrapper_nowait(*args, **kwargs):
            operation = Operation(fn, args, kwargs)
            with ClosingStack() as opiter_stack:
                return attempt_operation(get_opiter(operation), opiter_stack)

        @wraps(fn)
        def wrapper_operation(*args, **kwargs):
            return Operation(fn, args, kwargs)

        # TODO: docstring

        wrapper.nowait = wrapper_nowait
        wrapper.nowait.__name__ += ".nowait"
        wrapper.nowait.__qualname__ += ".nowait"
        wrapper.operation = wrapper_operation
        wrapper.operation.__name__ += ".operation"
        wrapper.operation.__qualname__ += ".operation"

        self._unbound = wrapper
        self.nowait = wrapper_nowait
        self.operation = wrapper_operation

    def __getattr__(self, attr):
        return getattr(self._fn, attr)

    def __eq__(self, other):
        if isinstance(other, atomic_operation):
            return self._fn == other._fn
        return NotImplemented

    def __repr__(self):
        return "<operation descriptor for {!r}>".format(self._fn)

    def __call__(self, *args, **kwargs):
        return self._unbound(*args, **kwargs)

    def _bind_instance(self, instance, owner):
        if instance is None:
            return self
        return BoundOperationMethod(
            self._unbound,
            instance,
            types.MethodType(self.nowait, instance),
            types.MethodType(self.operation, instance),
        )

    def _bind_class(self, instance, owner):
        return BoundOperationMethod(
            self._unbound,
            owner,
            types.MethodType(self.nowait, owner),
            types.MethodType(self.operation, owner),
        )

    def _bind_static(self, instance, owner):
        return self

    def __get__(self, instance, owner):
        # TODO: see if this cache is still giving us anything useful
        key = instance if instance is not None else owner
        try:
            cached_result, key_ref = self._cache[id(key)]
        except KeyError:
            pass
        else:
            if key_ref() is key:
                return cached_result
            del self._cache[id(key)]
        result = self._bind(instance, owner)
        self._cache[id(key)] = (result, weakref.ref(key))
        return result


@atomic_operation
def select(*operations):
    with ClosingStack() as toplevel_stack:
        for operation in operations:
            branch_stack = toplevel_stack.push(ClosingStack())
            try:
                return attempt_operation(get_opiter(operation), branch_stack)
            except _core.WouldBlock:
                pass

        handle = yield

        for branch_stack in toplevel_stack:
            publish_operation(handle, branch_stack)

        return (yield)
