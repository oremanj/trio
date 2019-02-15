import attr
import inspect
import outcome
import types
import warnings
from contextlib import closing
from functools import wraps, update_wrapper
from .. import _core
from ._ki import LOCALS_KEY_KI_PROTECTION_ENABLED, enable_ki_protection


__all__ = [
    "operation", "as_operation", "perform_operation", "attempt_operation"
]


@attr.s(cmp=False, slots=True)
class RootHandle:
    sleeping_task = attr.ib(default=None)
    completed = attr.ib(default=False)

    def _name(self):
        return "operation in {!r}".format(self.sleeping_task)

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


def _expect_yields_none(yielded, operation, description):
    if yielded is not None:
        raise TypeError(
            "operation {!r} must only yield None, but we got {!r} instead "
            "(at the {})".format(operation, yielded, description)
        )


def create_operation(fn, args, kwargs):
    operation = fn(*args, **kwargs)
    if not inspect.isgenerator(operation):
        raise TypeError(
            "operation {!r} must return a generator iterator, not {!r}"
            .format(fn, type(operation))
        )
    operation.gi_frame.f_locals.setdefault(
        LOCALS_KEY_KI_PROTECTION_ENABLED, True
    )
    return operation


def attempt_operation(operation):
    try:
        _expect_yields_none(
            operation.send(None), operation, "first suspension point"
        )
    except StopIteration as ex:
        return ex.value
    else:
        raise _core.WouldBlock


async def perform_operation(operation):
    await _core.checkpoint_if_cancelled()
    with closing(operation):
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
                    .format(self.fn.__qualname__)
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
        return await _core.wait_task_rescheduled(abort_fn)


def as_operation(operation):
    if inspect.isgenerator(operation):
        return operation
    elif (
        inspect.iscoroutine(operation)
        and operation.cr_frame.f_code is perform_operation.__code__
    ):
        return operation.cr_frame.f_locals["operation"]


@attr.s
class operation:
    fn = attr.ib()

    def __attrs_post_init__(self):
        update_wrapper(self.fn, self)

    def __get__(self, instance, owner):
        method = self.fn.__get__(instance, owner)

        @wraps(self.fn)
        def wrapper(*args, **kwargs):
            return perform_operation(create_operation(method, args, kwargs))

        @wraps(self.fn)
        def wrapper_nowait(*args, **kwargs):
            with closing(create_operation(method, args, kwargs)) as operation:
                return attempt_operation(operation)

        @wraps(self.fn)
        def wrapper_operation(*args, **kwargs):
            return create_operation(method, args, kwargs)

        wrapper.nowait = wrapper_nowait
        wrapper.operation = wrapper_operation
        return wrapper
