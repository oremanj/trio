import attr
import inspect
import outcome
import types
import warnings
import weakref
from contextlib import closing
from functools import wraps, update_wrapper
from .. import _core
from ._ki import LOCALS_KEY_KI_PROTECTION_ENABLED, enable_ki_protection


__all__ = ["operation", "get_operation"]


class SpuriousWakeup(BaseException):
    pass


@attr.s(cmp=False, slots=True)
class RootHandle:
    sleeping_task = attr.ib(default=None)
    completed = attr.ib(default=False)

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


@attr.s(repr=False, cmp=False, slots=True)
class CompletionHandle:
    _this_operation = attr.ib()
    _parent_handle = attr.ib()
    custom_sleep_data = attr.ib(default=None)

    def _name(self):
        return (
            self._parent_handle._name()
            + " -> " + self._this_operation.gi_code.co_name
        )

    def __repr__(self):
        return "<operation completion handle at {:#x}, for {}>".format(
            id(self), self._name()
        )

    def handle_for_suboperation(self, sub_operation):
        return CompletionHandle(sub_operation, self)

    @enable_ki_protection
    def complete(self, inner_value=None):
        try:
            self._this_operation.send(inner_value)
            raise TypeError(
                "operation {!r} improperly yielded after its second "
                "suspension point".format(self._this_operation)
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
                    "operation {!r} completion fail() was called with an "
                    "uninstantiable exception type {!r}"
                    .format(self._this_operation, inner_error)
                )
                inner_error.__cause__ = ex
        if not isinstance(inner_error, BaseException):
            inner_error = TypeError(
                "operation {!r} completion fail() was called with "
                "{!r} which is not an exception type or value"
                .format(self._this_operation, inner_error)
            )

        try:
            self._this_operation.throw(
                type(inner_error), inner_error, inner_error.__traceback__
            )
            raise TypeError(
                "operation {!r} improperly yielded after its second "
                "suspension point".format(self._this_operation)
            )
        except StopIteration as outer_result:
            self._parent_handle.complete(outer_result.value)
        except BaseException as outer_error:
            self._parent_handle.fail(outer_error)

    def retry(self):
        raise SpuriousWakeup


def qualname(fn):
    for attr in ("__qualname__", "__name__"):
        try:
            return getattr(fn, attr)
        except AttributeError:
            pass
    return repr(fn)


@attr.s(slots=True, repr=False)
class OperationFactory:
    fn = attr.ib()
    args = attr.ib()
    kwargs = attr.ib()
    _first_operation = attr.ib(default=None, init=False, cmp=False)

    def __attrs_post_init__(self):
        self._first_operation = self()

    def __repr__(self):
        args = list(map(repr, self.args)) + [
            "{}={!r}".format(*item) for item in self.kwargs.items()
        ]
        return "{}.operation({})".format(qualname(self.fn), ", ".join(args))

    def __call__(self):
        if self._first_operation is not None:
            operation = self._first_operation
            self._first_operation = None
            return operation

        operation = self.fn(*self.args, **self.kwargs)
        if not inspect.isgenerator(operation):
            raise TypeError(
                "operation {!r} must return a generator iterator, not {!r}"
                .format(qualname(fn), type(operation))
            )
        operation.gi_frame.f_locals.setdefault(
            LOCALS_KEY_KI_PROTECTION_ENABLED, True
        )
        return operation

    __iter__ = __call__


def _expect_yields_none(yielded, operation, description):
    if yielded is not None:
        raise TypeError(
            "operation {!r} must only yield None, but we got {!r} instead "
            "(at the {})".format(operation, yielded, description)
        )


def get_operation(factory):
    if isinstance(factory, OperationFactory):
        return factory()
    if (
        inspect.iscoroutine(factory)
        and factory.cr_frame.f_code is perform_operation.__code__
    ):
        factory = factory.cr_frame.f_locals["operation_factory"]
        raise TypeError(
            "use {0}.operation(...), not {0}(...)".format(qualname(factory.fn))
        )
    raise TypeError(
        "need an operation factory, not {!r}".format(qualname(type(factory)))
    )


def attempt_operation(operation):
    try:
        _expect_yields_none(
            operation.send(None), operation, "first suspension point"
        )
    except StopIteration as ex:
        return ex.value
    else:
        raise _core.WouldBlock


# The fact that this argument is named "operation_factory" is relied upon
# to generate a better error message in get_operation().
async def perform_operation(operation_factory):
    while True:
        await _core.checkpoint_if_cancelled()
        with closing(operation_factory()) as operation:
            try:
                potential_immediate_result = attempt_operation(operation)
            except _core.WouldBlock:
                pass
            except:
                await _core.cancel_shielded_checkpoint()
                raise
            else:
                await _core.cancel_shielded_checkpoint()
                return potential_immediate_result

            root_handle = RootHandle()
            completion_handle = CompletionHandle(operation, root_handle)
            try:
                try:
                    _expect_yields_none(
                        operation.send(completion_handle),
                        operation,
                        "second suspension point"
                    )
                except StopIteration as ex:
                    raise TypeError(
                        "@operation function {!r} should have yielded twice, "
                        "but instead it returned after yielding once"
                        .format(qualname(operation_factory.fn))
                    ) from ex
            except:
                await _core.cancel_shielded_checkpoint()
                raise

            def abort_fn(raise_cancel):
                try:
                    operation.close()
                except BaseException as ex:
                    root_handle.fail(ex)
                    return _core.Abort.FAILED
                else:
                    return _core.Abort.SUCCEEDED

            root_handle.sleeping_task = _core.current_task()
            try:
                return await _core.wait_task_rescheduled(abort_fn)
            except SpuriousWakeup:
                pass


class operation:
    def __init__(self, fn):
        if not callable(fn):
            raise TypeError("@operation must be used as a decorator")
        self._fn = fn
        self._cache = weakref.WeakKeyDictionary()
        update_wrapper(self, self._fn, updated=())
        self._unbound = self.__get__(None, None)
        self.nowait = self._unbound.nowait
        self.operation = self._unbound.operation

    def __getattr__(self, attr):
        return getattr(self._fn, attr)

    def __eq__(self, other):
        if isinstance(other, operation):
            return self._fn == other._fn
        return NotImplemented

    def __repr__(self):
        return "<operation descriptor for {!r}>".format(self._fn)

    def __call__(self, *args, **kwargs):
        return self._unbound(*args, **kwargs)

    def __get__(self, instance, owner):
        key = instance if instance is not None else owner
        try:
            if key is not None:
                return self._cache[key]
        except KeyError:
            pass

        if key is not None and hasattr(self._fn, "__get__"):
            method = self._fn.__get__(instance, owner)
        else:
            method = self._fn

        @wraps(self._fn)
        def wrapper(*args, **kwargs):
            return perform_operation(OperationFactory(method, args, kwargs))

        @wraps(self._fn)
        def wrapper_nowait(*args, **kwargs):
            factory = OperationFactory(method, args, kwargs)
            with closing(factory()) as operation:
                return attempt_operation(operation)

        @wraps(self._fn)
        def wrapper_operation(*args, **kwargs):
            return OperationFactory(method, args, kwargs)

        wrapper.nowait = wrapper_nowait
        wrapper.nowait.__name__ += ".nowait"
        wrapper.nowait.__qualname__ += ".nowait"
        wrapper.operation = wrapper_operation
        wrapper.operation.__name__ += ".operation"
        wrapper.operation.__qualname__ += ".operation"

        if key is not None:
            self._cache[key] = wrapper
        return wrapper
