import attr
import collections
import contextvars
import inspect
import outcome
import types
import weakref
from contextlib import closing, contextmanager
from functools import wraps, update_wrapper, partial
from .. import _core
from ._ki import LOCALS_KEY_KI_PROTECTION_ENABLED, enable_ki_protection


__all__ = ["atomic_operation", "reversible_operation", "select"]


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
    """Provides the ability to complete an operation.

    When an operation is not able to complete synchronously, it
    receives a completion handle at its first yield point. It is
    expected to "publish" this handle so that some other task can
    later call :meth:`complete` or :meth:`fail` to unblock the task
    that was performing the operation. (For example, if you're sending
    on a channel with no one waiting to receive, you would publish
    your completion handle somewhere that the next task that wants to
    receive would be able to invoke it.)

    Each invocation of an operation function gets its own completion
    handle, so the identity of the handle can (and should) be used to
    distinguish which operation you're talking about.

    Completion handles are arranged in a tree, based on the structure
    of operation functions wrapping other operation functions.
    For example, if you perform the ``outer()`` operation in::

        @atomic_operation
        def outer(self):
            self.outer_handle = yield self.inner()
            return (yield) + 2

        @atomic_operation
        def inner(self):
            self.inner_handle = yield
            return (yield) * 3

    then the parent of ``self.inner_handle`` is ``self.outer_handle``.
    Calling ``self.inner_handle.complete(10)`` would send 10 in at the
    ``inner()`` final yield point, resulting in ``inner()`` returning 30,
    which would be sent in at the ``outer()`` final yield point, so that
    ``outer()`` would return 32. Calling ``self.outer_handle.complete(10)``
    would close the ``inner()`` iterator and send 10 in at the ``outer()``
    final yield point, resulting in ``outer()`` returning 12.

    """

    # The operation iterator that this completion handle applies to.
    # The value or error passed to :meth:`complete` or :meth:`fail`
    # will be sent or thrown into this iterator, which should be
    # suspended at its final yield point by the time those methods
    # are called. The resulting return value or exception will be
    # passed along to our parent handle.
    _opiter = attr.ib()

    # The completion handle that should receive the value or error
    # returned or raised by ``_opiter``. This may be anything that
    # defines :meth:`_complete`, :meth:`_fail`, :meth:`retry`, and
    # :meth:`add_async_cleanup` methods.  Every
    # :class:`CompletionHandle` has a valid parent, so the root of the
    # tree created to perform an operation must be a different type;
    # see :class:`RootHandle` below.
    _parent_handle = attr.ib()

    # A synchronous callable that will be invoked when :meth:`complete`
    # or :meth:`fail` is called on this operation. It should close
    # all children of this operation.
    _close_children_fn = attr.ib()

    # The task that this operation will ultimately wake up.
    # We ensure that the unpublish portion of the operation
    # executes in a context as if it were running as this task.
    _task = attr.ib(factory=lambda: _core.current_task())

    #: Trio does not assign any meaning to this attribute; it's provided
    #: to allow for application-defined coordination between the sleeping
    #: and waking tasks.
    custom_sleep_data = attr.ib(default=None)

    def __str__(self):
        return (
            str(self._parent_handle) + " -> " + self._opiter.gi_code.co_name
        )

    def __repr__(self):
        return "<operation completion handle at {:#x}, for {}>".format(
            id(self), str(self)
        )

    @contextmanager
    def _fake_current_task(self):
        waker = getattr(_core._run.GLOBAL_RUN_CONTEXT, "task", None)
        if waker is self._task:
            raise RuntimeError(
                "can't complete an operation from within its own "
                "operation function"
            )
        _core._run.GLOBAL_RUN_CONTEXT.task = self._task
        try:
            yield
        finally:
            if waker is not None:
                _core._run.GLOBAL_RUN_CONTEXT.task = waker
            else:
                del _core._run.GLOBAL_RUN_CONTEXT.task

    @enable_ki_protection
    def complete(self, inner_value=None):
        """Complete this operation, sending the value ``inner_value`` in to
        its operation iterator at the final yield point. The resulting return
        value will be passed along to our parent handle's :meth:`complete`
        method. If an exception is raised, it will be passed to our parent
        handle's :meth:`fail` method.

        Args:
          inner_value (object): The value to send in to the suspended
              operation function.

        Raises:
          Nothing; all errors are forwarded to the suspended operation
          function.

        """
        with self._fake_current_task():
            try:
                self._task.context.run(self._close_children_fn)
            except BaseException as ex:
                exc = ex
            else:
                self._task.context.run(self._complete, inner_value)
                return
        self.fail(exc)

    def _complete(self, inner_value):
        try:
            self._opiter.send(inner_value)
            raise_through_opiter(
                self._opiter,
                RuntimeError("operation function yielded too many times"),
            )
        except StopIteration as outer_result:
            self._parent_handle._complete(outer_result.value)
        except BaseException as outer_error:
            self._parent_handle._fail(outer_error)

    @enable_ki_protection
    def fail(self, inner_error):
        """Fail this operation, throwing the exception ``inner_error`` in to
        its operation iterator at the final yield point. The exception that
        propagates out will be passed along to our parent handle's :meth:`fail`
        method. If no exception propagates out, the operation function's
        return value will be passed to our parent handle's :meth:`complete`
        method instead.

        Args:
          inner_error (:class:`BaseException` instance or subclass): The
              exception to throw in to the suspended operation
              function.  If a type, it will be called with zero
              arguments to produce a value. If not an exception, we
              will throw in a TypeError instead.

        Raises:
          Nothing; all errors are forwarded to the suspended operation
          function.

        """
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
                "{!r} which is not an exception"
                .format(str(self), inner_error)
            )

        with self._fake_current_task():
            try:
                self._task.context.run(self._close_children_fn)
            except BaseException as ex:
                inner_error.__context__ = ex
            self._task.context.run(self._fail, inner_error)

    def _fail(self, inner_error):
        try:
            raise_through_opiter(self._opiter, inner_error, may_swallow=True)
        except StopIteration as outer_result:
            self._parent_handle._complete(outer_result.value)
        except BaseException as outer_error:
            self._parent_handle._fail(outer_error)

    def retry(self):
        """Request that this operation be retried.

        This may only be invoked from an operation function that has just
        been resumed from its final yield point due to a call to
        :meth:`complete` or :meth:`fail`. It results in the entire top-level
        operation being unwound and retried from the beginning. This is useful
        for dealing with false wakeups when they might occur (e.g. with
        UNIX non-blocking semantics).

        Returns:
          This function never returns; it raises an internal exception type
          used to implement the retry semantics.

        """
        self._parent_handle.retry()

    def add_async_cleanup(self, async_fn):
        """Request that ``await async_fn()`` be invoked in the context
        of the task that's performing this operation, after the operation
        completes but before control progresses past it.

        This may only be invoked from an operation function that has
        just been resumed from its final yield point due to a call to
        :meth:`complete` or :meth:`fail`.  ``async_fn`` will be
        invoked within a shielded cancel scope.  It cannot change the
        return value of the operation and cannot prevent the operation
        from completing, but if it throws an exception, that exception
        will propagate out of the operation call. If multiple async
        cleanup functions are registered, they will be called in the
        order in which they were registered.

        Args:
          async_fn: A zero-argument async function. (Use
              :func:`functools.partial` if you need to pass arguments.)

        """
        self._parent_handle.add_async_cleanup(async_fn)


@attr.s
class ClosingStack(list):
    """A list of resources with ``close()`` methods, which supports closing
    all of them with appropriate exception chaining (like ExitStack).
    """

    def close(self):
        """Call the ``close()`` method of each resource that has been
        pushed onto the stack, in reverse of the order in which they
        were pushed.
        """
        self.close_from(0)

    def close_from(self, index):
        """Call the ``close()`` method of each resource at or after
        index ``index`` in the stack, in reverse of the order in which
        they were pushed. The items remain on the stack.
        """
        last_exc = None
        for item in reversed(self[index:]):
            try:
                try:
                    if last_exc is not None:
                        raise last_exc
                finally:
                    item.close()
            except BaseException as new_exc:
                last_exc = new_exc
        if last_exc is not None:
            raise last_exc

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


@attr.s(slots=True, repr=False)
class Operation(collections.abc.Coroutine):
    """A potentially-blocking atomic operation that is structured
    so as to permit composition and nonblocking attempts.

    An operation is created by calling an *operation function*,
    which is defined by writing a generator decorated with
    ``@atomic_operation``. Once you have an operation ``op``, you
    can say:

      * ``op.attempt()``, to attempt to perform the operation without
        blocking, raising :exc:`~trio.WouldBlock` if that's not
        possible
      * ``await op.perform()``, to perform the operation and wait for
        it to complete
      * ``op | transform`` to produce a new operation that becomes
        completed when ``op`` does, and whose result is determined
        by passing the result of ``op`` through the single-argument
        callable ``transform``
      * ``op.and_raise(SomeError, *args)`` to produce a new operation
        that becomes completed when ``op`` does, and whose result
        is to raise ``SomeError(*args)``
      * ``select(op1, op2, ...)``, to produce a new operation;
        performing or attempting the new operation performs or
        attempts ``op1``, ``op2``, etc, in parallel, evaluates to the
        result of the first of them that completes, and ensures that
        no more than one of them completes

    These can be done with the same operation as many times as you
    like.  Operations also can be treated like a coroutine for
    :meth:`perform`, so ``await op`` is equivalent to ``await
    op.perform()`` except that you can only do it once per operation
    object. (This support is intended to allow you to turn a normal
    async function into an operation function without breaking
    backwards compatibility; typically you wouldn't actually extract
    the operation object and await it as separate operations, but
    would just say ``await some_operation_func()``.)

    If an operation function is called and nothing is done with the
    result (it is not attempted, performed, transformed, etc), you
    will get a "coroutine never awaited" warning, just like with a
    regular async function. Call :meth:`close` to suppress the warning
    if you're doing this deliberately.
    """

    #: The underlying operation function. Calling this will return an
    #: operation iterator, not an Operation object.
    func = attr.ib()

    #: The positional arguments we will pass to :attr:`func`.
    args = attr.ib()

    #: The keyword arguments we will pass to :attr:`func`.
    keywords = attr.ib()

    # The result of an initial call to func(*args, **keywords) performed
    # during construction, or None if it has already been consumed.
    # This allows TypeErrors from calling a function with the wrong
    # signature to be raised immediately rather than only later when
    # the operation is performed.
    _first_opiter = attr.ib(default=None, init=False, cmp=False)

    # A coroutine for perform_operation(self), created during construction.
    _coro = attr.ib(default=None, init=False, cmp=False)

    def __attrs_post_init__(self):
        self._first_opiter = self._get_opiter()
        self._coro = perform_operation(self)

        # Set the name of the coroutine object based on the name of
        # the function, so that the "coroutine ___ was never awaited"
        # message contains a useful name rather than just
        # "perform_operation".
        if hasattr(self.func, "__qualname__"):
            self._coro.__qualname__ = self.func.__qualname__
            self._coro.__name__ = self.func.__name__
        else:
            self._coro.__qualname__ = repr(self.func)
            self._coro.__name__ = repr(self.func)

    @property
    def name(self):
        """A human-readable name describing this operation.
        Initially this is the name of the operation function; if you
        wrapped an operation using ``|``, it will contain both the
        name of the underlying operation function and the name of the
        wrapper. You can also set it to something else. The name shows
        up in the ``str()`` and ``repr()``, as well as in
        "RuntimeError: coroutine ... was never awaited" warnings.
        """
        return self._coro.__qualname__

    @property
    def __name__(self):
        return self._coro.__name__

    @name.setter
    def name(self, value):
        self._coro.__qualname__ = value
        self._coro.__name__ = value

    def __str__(self):
        # Using this instead of straight repr() lets select(a(), b())
        # repr as "<operation select(a(), b())>" rather than as
        # "<operation select(<operation a()>, <operation b()>)>".
        def fmt(arg):
            return str(arg) if isinstance(arg, Operation) else repr(arg)

        args = list(map(fmt, self.args)) + [
            "{}={}".format(key, fmt(value))
            for key, value in self.keywords.items()
        ]
        return "{}({})".format(self.name, ", ".join(args))

    def __repr__(self):
        return "<operation {} at {:#x}>".format(str(self), id(self))

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
                .format(self.func, type(opiter))
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

    def __or__(self, transform):
        if not callable(transform):
            return NotImplemented

        self._discard_coro("|")
        func = self.func

        def combined(*args, **kwargs):
            return transform((yield from func(*args, **kwargs)))

        for attr in ("__qualname__", "__name__"):
            try:
                transform_name = getattr(transform, attr)
                break
            except AttributeError:
                pass
        else:
            transform_name = repr(transform)
        base_name = self.name
        if base_name[:1] == '(' and base_name[-1:] == ')':
            base_name = base_name[1:-1]
        combined_op = Operation(combined, self.args, self.keywords)
        combined_op.name = "({} | {})".format(base_name, transform_name)
        return combined_op

    def and_raise(self, exc_type, *exc_args):

        def raise_it(_):
            raise exc_type(*exc_args)

        self.discard_coro("and_raise()")
        raise_it.__qualname__ = "<raise {}>".format(exc_type.__name__)
        return self | raise_it

    def tagged(self, tag):

        def tag_it(result):
            return (tag, result)

        self.discard_coro("tagged()")
        tag_it.__qualname__ = "({!r}, _)".format(tag)
        return self | tag_it


def attempt_operation(opiter, opiter_stack):
    """Attempt to complete the operation described by ``opiter`` without
    blocking.

    Args:
      opiter: An operation iterator that has not yet been started.
      opiter_stack (ClosingStack): ``opiter`` and its children (if any)
          will be pushed onto this stack if the operation cannot be
          completed immediately.

    Returns:
      The value returned by ``opiter`` or one of its children before
      their first yield point.

    Raises:
      RuntimeError: if ``opiter`` or any of its children yields
          something unexpected at its first yield point
      trio.WouldBlock: if nothing went wrong but the operation could not
          be completed immediately
      Anything else: that was raised by ``opiter`` or one of its children
          before their first yield point
    """
    try:
        first_yield = opiter.send(None)
    except StopIteration as ex:
        return ex.value

    opiter_stack.append(opiter)
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
    """Advance a stack of operation iterators from the first yield point
    to the second, passing them a corresponding stack of
    completion handles with ``handle`` as the parent of the top one.

    Args:
      handle: An operation completion handle that should receive the
          ultimate result of this operation stack.
      opiter_stack (ClosingStack): The stack of operation iterators
          to advance.

    Raises:
      RuntimeError: if any of the operation functions in ``opiter_stack``
          raises something unexpected at its second yield point, or returns
          after its first yield point
      Anything else: that was raised by any of the operation functions in
          ``opiter_stack`` between its first and second yield point
    """
    for idx, opiter in enumerate(opiter_stack):
        handle = CompletionHandle(
            opiter, handle, partial(opiter_stack.close_from, idx + 1)
        )
        try:
            second_yield = opiter.send(handle)
        except StopIteration as ex:
            raise RuntimeError(
                "operation function yielded too few times"
            ) from None
        if second_yield is not None:
            raise_through_opiter(
                opiter,
                RuntimeError(
                    "operation function yielded {!r} at its second yield "
                    "point; expected None".format(second_yield)
                ),
            )


class SpuriousWakeup(BaseException):
    """Internal exception thrown to implement operation retry."""


@attr.s(cmp=False, slots=True)
class RootHandle:
    """Implementation of the CompletionHandle interface that wakes up a
    sleeping task to deliver the value or error.
    """

    # The sleeping task to be woken up, or None if it isn't sleeping yet
    sleeping_task = attr.ib(default=None)

    # True if we've already woken up the sleeping task
    completed = attr.ib(default=False)

    # A list of async cleanup functions to invoke from within the
    # sleeping task after it's woken up, in the order in which they
    # appear in the list.
    async_cleanup_fns = attr.ib(factory=list)

    def __str__(self):
        return getattr(self.sleeping_task, "name", "[setting up]")

    def reschedule(self, operation_outcome):
        if self.completed:
            raise RuntimeError("that operation has already been completed")
        _core.reschedule(self.sleeping_task, operation_outcome)
        self.completed = True
        self.sleeping_task = None

    def _complete(self, value):
        self.reschedule(outcome.Value(value))

    def _fail(self, error):
        self.reschedule(outcome.Error(error))

    def retry(self):
        if self.sleeping_task is None or self.completed:
            raise RuntimeError(
                "retry() must be called after the second yield point in an "
                "operation function"
            )
        raise SpuriousWakeup

    def add_async_cleanup(self, async_fn, *args):
        if self.sleeping_task is None or self.completed:
            raise RuntimeError(
                "add_async_cleanup() must be called after the second "
                "yield point in an operation function"
            )
        self.async_cleanup_fns.append(partial(async_fn, *args))


async def perform_operation(operation):
    """Perform the given operation, blocking if necessary.

    Args:
      operation (Operation): The operation to perform.

    Returns:
      The result of the operation.

    Raises:
      Any error raised by the operation, or :exc:`RuntimeError` if the
      operation function violated its contract (did not yield either
      zero or two times, yielded something other than None or another
      operation at its first yield point, yielded something other than
      None at its second yield point, etc).
    """
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
                    if root_handle.async_cleanup_fns:
                        with _core.CancelScope(shield=True):
                            last_exc = None
                            for fn in root_handle.async_cleanup_fns:
                                try:
                                    await fn()
                                except BaseException as new_exc:
                                    if (
                                        new_exc.__context__ is None
                                        and last_exc is not None
                                    ):
                                        new_exc.__context__ = last_exc
                                    last_exc = new_exc
                            if last_exc is not None:
                                raise last_exc
    finally:
        if not did_block:
            await _core.cancel_shielded_checkpoint()


def atomic_operation(fn):
    """Decorator that turns a generator into an operation function."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return Operation(fn, args, kwargs)
    return wrapper


def reversible_operation(fn):
    """Decorator that turns an async generator into an operation function.

    The async generator should yield exactly once, providing its
    intended return value. If the operation should be committed, the
    async generator will be resumed normally; otherwise, it will be
    closed (internally raising :exc:`GeneratorExit`). This is helpful
    for operations (such as receiving on a buffered stream) which are
    cancellation-safe and reversible but can't easily be made to
    follow the ``@atomic_operation`` protocol.
    """

    @atomic_operation
    @wraps(fn)
    def wrapper(*args, **kwargs):
        agen = fn(*args, **kwargs)
        if not async_generator.isasyncgen(agen):
            raise TypeError(
                "@reversible_operation {} must be an async generator function"
                .format(fn)
            )
        handle = yield
        cancel_scope = _core.CancelScope(shield=True)
        blocking_task = _core.current_task()

        async def run_in_system_task():
            _core.current_task().context = blocking_task.context
            with cancel_scope:
                result = await outcome.acapture(agen.asend, None)
            if cancel_scope.cancel_called:
                @handle.add_async_cleanup
                async def cleanup():
                    result.unwrap()
                    await agen.aclose()  # only needed if no exception
            else:
                try:
                    handle.complete(result.unwrap())
                except BaseException as ex:
                    handle.fail(ex)
                else:
                    handle.add_async_cleanup(agen.aclose)

        _core.spawn_system_task(
            run_in_system_task, name="<reversible_operation {}>".format(fn)
        )
        try:
            return (yield)
        finally:
            cancel_scope.cancel()

    return wrapper


@atomic_operation
def select(*operations):
    """Perform exactly one of the given operations, returning its result.

    Use :meth:`~Operation.tagged` if you need to distinguish between
    different operations that might return otherwise indistinguishable
    values.

    :func:`select` on zero operations blocks until it is cancelled.
    """
    with ClosingStack() as toplevel_stack:
        opiters = [iter(op) for op in operations]

        for opiter in opiters:
            branch_stack = ClosingStack()
            toplevel_stack.append(branch_stack)
            try:
                return attempt_operation(opiter, branch_stack)
            except _core.WouldBlock:
                pass

        handle = yield

        for branch_stack in toplevel_stack:
            publish_operation(handle, branch_stack)

        return (yield)
