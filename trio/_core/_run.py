import functools
import itertools
import logging
import os
import random
import select
import sys
import threading
import weakref
from collections import deque
import collections.abc
from contextlib import contextmanager, closing, ExitStack

from contextvars import copy_context
from math import inf
from time import perf_counter

from sniffio import current_async_library_cvar

import attr
from async_generator import isasyncgen
from sortedcontainers import SortedDict
from outcome import Error, Value, capture

from . import _public
from ._entry_queue import EntryQueue, TrioToken
from ._exceptions import (TrioInternalError, RunFinishedError, Cancelled)
from ._ki import (
    LOCALS_KEY_KI_PROTECTION_ENABLED, ki_manager, enable_ki_protection
)
from ._multierror import MultiError
from ._traps import (
    Abort,
    wait_task_rescheduled,
    CancelShieldedCheckpoint,
    PermanentlyDetachCoroutineObject,
    WaitTaskRescheduled,
)
from .. import _core
from .._deprecate import deprecated

# At the bottom of this file there's also some "clever" code that generates
# wrapper functions for runner and io manager methods, and adds them to
# __all__. These are all re-exported as part of the 'trio' or 'trio.hazmat'
# namespaces.
__all__ = [
    "Task", "run", "open_nursery", "open_cancel_scope", "CancelScope",
    "checkpoint", "current_task", "current_effective_deadline",
    "checkpoint_if_cancelled", "TASK_STATUS_IGNORED"
]

GLOBAL_RUN_CONTEXT = threading.local()

if os.name == "nt":
    from ._io_windows import WindowsIOManager as TheIOManager
elif hasattr(select, "epoll"):
    from ._io_epoll import EpollIOManager as TheIOManager
elif hasattr(select, "kqueue"):
    from ._io_kqueue import KqueueIOManager as TheIOManager
else:  # pragma: no cover
    raise NotImplementedError("unsupported platform")

# When running under Hypothesis, we want examples to be reproducible and
# shrinkable.  pytest-trio's Hypothesis integration monkeypatches this
# variable to True, and registers the Random instance _r for Hypothesis
# to manage for each test case, which together should make Trio's task
# scheduling loop deterministic.  We have a test for that, of course.
_ALLOW_DETERMINISTIC_SCHEDULING = False
_r = random.Random()

# Used to log exceptions in instruments
INSTRUMENT_LOGGER = logging.getLogger("trio.abc.Instrument")


# On 3.7+, Context.run() is implemented in C and doesn't show up in
# tracebacks. On 3.6 and earlier, we use the contextvars backport, which is
# currently implemented in Python and adds 1 frame to tracebacks. So this
# function is a super-overkill version of "0 if sys.version_info >= (3, 7)
# else 1". But if Context.run ever changes, we'll be ready!
#
# This can all be removed once we drop support for 3.6.
def _count_context_run_tb_frames():
    def function_with_unique_name_xyzzy():
        1 / 0

    ctx = copy_context()
    try:
        ctx.run(function_with_unique_name_xyzzy)
    except ZeroDivisionError as exc:
        tb = exc.__traceback__
        # Skip the frame where we caught it
        tb = tb.tb_next
        count = 0
        while tb.tb_frame.f_code.co_name != "function_with_unique_name_xyzzy":
            tb = tb.tb_next
            count += 1
        return count


CONTEXT_RUN_TB_FRAMES = _count_context_run_tb_frames()


@attr.s(frozen=True)
class SystemClock:
    # Add a large random offset to our clock to ensure that if people
    # accidentally call time.perf_counter() directly or start comparing clocks
    # between different runs, then they'll notice the bug quickly:
    offset = attr.ib(default=attr.Factory(lambda: _r.uniform(10000, 200000)))

    def start_clock(self):
        pass

    # In cPython 3, on every platform except Windows, perf_counter is
    # exactly the same as time.monotonic; and on Windows, it uses
    # QueryPerformanceCounter instead of GetTickCount64.
    def current_time(self):
        return self.offset + perf_counter()

    def deadline_to_sleep_time(self, deadline):
        return deadline - self.current_time()


################################################################
# CancelScope and friends
################################################################


@attr.s(cmp=False, repr=False, slots=True)
class CancelScope:
    """A *cancellation scope*: the link between a unit of cancellable
    work and Trio's cancellation system.

    A :class:`CancelScope` becomes associated with some cancellable work
    when it is used as a context manager surrounding that work::

        cancel_scope = trio.CancelScope()
        ...
        with cancel_scope:
            await long_running_operation()

    Inside the ``with`` block, a cancellation of ``cancel_scope`` (via
    a call to its :meth:`cancel` method or via the expiry of its
    :attr:`deadline`) will immediately interrupt the
    ``long_running_operation()`` by raising :exc:`Cancelled` at its
    next :ref:`checkpoint <checkpoints>`.

    The context manager ``__enter__`` returns the :class:`CancelScope`
    object itself, so you can also write ``with trio.CancelScope() as
    cancel_scope:``.

    If a cancel scope becomes cancelled before entering its ``with`` block,
    the :exc:`Cancelled` exception will be raised at the first
    checkpoint inside the ``with`` block. This allows a
    :class:`CancelScope` to be created in one :ref:`task <tasks>` and
    passed to another, so that the first task can later cancel some work
    inside the second.

    Cancel scopes are reusable: once you exit the ``with`` block, you
    can use the same :class:`CancelScope` object to wrap another chunk
    of work.  (The cancellation state doesn't change; once a cancel
    scope becomes cancelled, it stays cancelled.)  This can be useful
    if you want a cancellation to be able to interrupt some operations
    in a loop but not others::

        cancel_scope = trio.CancelScope(deadline=...)
        while True:
            with cancel_scope:
                request = await get_next_request()
                response = await handle_request(request)
            await send_response(response)

    Cancel scopes are *not* reentrant: you can't enter a second
    ``with`` block using the same :class:`CancelScope` while the first
    one is still active. (You'll get a :exc:`RuntimeError` if you
    try.)  If you want multiple blocks of work to be cancelled with
    the same call to :meth:`cancel` or at the expiry of the same
    deadline, see the :meth:`linked_child` method.

    The :class:`CancelScope` constructor takes initial values for the
    cancel scope's :attr:`deadline`, :attr:`shield`,
    :attr:`shield_during_cleanup`, and :attr:`grace_period`
    attributes; these may be freely modified after construction,
    whether or not the scope has been entered yet, and changes take
    immediate effect.

    """

    _tasks = attr.ib(factory=set, init=False)
    _scope_task = attr.ib(default=None, init=False)
    cancel_called = attr.ib(default=False, init=False)
    cleanup_expired = attr.ib(default=False, init=False)
    cancelled_caught = attr.ib(default=False, init=False)

    # The time at which this cancel scope logically becomes cancelled:
    # the minimum deadline of this cancel scope and any of its linked
    # parents, recursively. This is inf if cancel() has already been
    # called.
    _effective_deadline = attr.ib(default=inf, init=False)

    # The deadline that is associated with this cancel scope in
    # Runner.deadlines: _effective_deadline if self._tasks is
    # nonempty, else inf (meaning "not registered").
    _registered_deadline = attr.ib(default=inf, init=False)

    _cleanup_started_at = attr.ib(default=inf, init=False)
    _local_grace_period = attr.ib(default=None, init=False)

    # Most cancel scopes don't have linked children; those that do
    # will replace this empty tuple with a WeakSet
    _linked_children = attr.ib(default=(), init=False)
    _linked_parent = attr.ib(default=None, init=False)

    # Constructor arguments:
    _deadline = attr.ib(default=inf, kw_only=True)
    _shield = attr.ib(default=False, kw_only=True)
    _shield_during_cleanup = attr.ib(default=False, kw_only=True)
    _grace_period = attr.ib(default=None, kw_only=True)

    def __attrs_post_init__(self):
        self._effective_deadline = self._deadline

    @enable_ki_protection
    def __enter__(self):
        task = _core.current_task()
        if self._scope_task is not None:
            raise RuntimeError(
                "cancel scope may not be entered while it is already "
                "active{}; try `with cancel_scope.linked_child() as "
                "new_scope:` instead".format(
                    "" if self._scope_task is task else
                    " in another task ({!r})".format(self._scope_task.name)
                )
            )
        self._scope_task = task
        self.cancelled_caught = False
        self._add_task(task)
        self._update_computed_deadlines()  # we now have some tasks
        return self

    @enable_ki_protection
    def __exit__(self, etype, exc, tb):
        # NB: NurseryManager calls _close() directly rather than __exit__(),
        # so __exit__() must be just _close() plus this logic for adapting
        # the exception-filtering result to the context manager API.

        # Tracebacks show the 'raise' line below out of context, so let's give
        # this variable a name that makes sense out of context.
        remaining_error_after_cancel_scope = self._close(exc)
        if remaining_error_after_cancel_scope is None:
            return True
        elif remaining_error_after_cancel_scope is exc:
            return False
        else:
            # Copied verbatim from MultiErrorCatcher.  Python doesn't
            # allow us to encapsulate this __context__ fixup.
            old_context = remaining_error_after_cancel_scope.__context__
            try:
                raise remaining_error_after_cancel_scope
            finally:
                _, value, _ = sys.exc_info()
                assert value is remaining_error_after_cancel_scope
                value.__context__ = old_context

    def linked_child(
        self,
        *,
        deadline=inf,
        shield=None,
        shield_during_cleanup=None,
        grace_period=None,
    ):
        """Return another :class:`CancelScope` object that automatically
        becomes cancelled when this one does.

        We say that the returned cancel scope is a "linked child" of
        this "parent" cancel scope.  Note that this relationship has
        nothing to do with lexical nesting of ``with`` blocks.

        The relationship between the parent cancel scope and its new
        linked child is one-way: if the parent cancel scope becomes
        cancelled, all of its linked children do too, but any of the
        children can independently become cancelled without affecting
        the parent. Each child has its own :attr:`deadline`, which is
        :data:`math.inf` (not inherited from the parent scope) if the
        ``deadline`` argument is unspecified; the expiry of a linked
        child's deadline cancels that child only, while the expiry of
        the parent's deadline cancels the parent as well as all linked
        children.

        The new linked child inherits its non-:attr:`deadline` mutable
        attributes (:attr:`shield`, :attr:`~CancelScope.shield_during_cleanup`,
        :attr:`grace_period`) from the parent, unless overridden by a
        corresponding argument to :meth:`linked_child`.  The child's
        version of these attributes may be changed without affecting the
        parent, but changes to the parent's version will be propagated
        to all children, overriding any local value they've previously
        set.

        If a linked child is created from a scope that is already
        cancelled, the child is considered to have been cancelled at the
        same time as its parent was. If the child's effective grace
        period is different from the parent's, this may result in a
        different time at which the grace period is considered to
        expire.  A per-cancellation grace period specified with
        ``cancel(grace_period=...)`` is propagated to linked children as
        well, including those that don't yet exist at the time of the
        call to :meth:`cancel`.

        Multiple layers of cancel scope linkage are supported.  The
        cancellation of any parent scope affects all its linked
        children, all their linked children, and so on.

        """

        if self._linked_children == ():
            # We keep _linked_children as an empty tuple until the
            # first linked_child is created, to avoid creating a
            # fairly heavyweight WeakSet on every cancel scope object
            self._linked_children = weakref.WeakSet()

        if shield is None:
            shield = self._shield
        if shield_during_cleanup is None:
            shield_during_cleanup = self._shield_during_cleanup
        if grace_period is None:
            grace_period = self._grace_period

        child = CancelScope()
        child._linked_parent = self
        self._linked_children.add(child)
        child._deadline = deadline
        child._shield = shield
        child._shield_during_cleanup = shield_during_cleanup
        child._grace_period = grace_period
        if self.cancel_called:
            child.cancel_called = True
            child._local_grace_period = self._local_grace_period
        child._update_computed_deadlines()
        return child

    def __repr__(self):
        if self._scope_task is None:
            binding = "unbound"
        else:
            binding = "bound to {!r}".format(self._scope_task.name)
            if len(self._tasks) > 1:
                binding += " and its {} descendant{}".format(
                    len(self._tasks) - 1, "s" if len(self._tasks) > 2 else ""
                )

        if self.cleanup_expired:
            state = ", cancelled"
        elif self.cancel_called:
            state = ", cancelled pending cleanup"
        else:
            state = ""

        if self._effective_deadline != inf:
            try:
                now = current_time()
            except RuntimeError:  # must be called from async context
                deadline = ""
            else:
                deadline = ", deadline is {:.2f} seconds {}".format(
                    abs(self.deadline - now),
                    "from now" if self.deadline >= now else "ago"
                )
        else:
            deadline = ""

        if self._linked_children:
            count = len(self._linked_children)
            linked_children = ", {} linked child{}".format(
                count, "ren" if count > 1 else ""
            )
        else:
            linked_children = ""

        return "<trio.CancelScope at {:#x}, {}{}{}{}>".format(
            id(self), binding, state, deadline, linked_children
        )

    @enable_ki_protection
    def _update_computed_deadlines(self):
        """Recompute self._effective_deadline, and update our linked
        children if it changed. Recompute self._registered_deadline,
        and update the Runner.deadlines map if it changed.

        Call this after changing self._deadline, self.cancel_called,
        or bool(self._tasks).
        """
        old_effective = self._effective_deadline
        old_registered = self._registered_deadline

        if self.cleanup_expired:
            new_effective = inf
        elif self.cancel_called:
            new_effective = (
                self._cleanup_started_at +
                self._local_effective_grace_period
            )
        else:
            new_effective = self._deadline
            if self._linked_parent is not None:
                new_effective = min(
                    new_effective, self._linked_parent._effective_deadline
                )

        if not self._tasks:
            new_registered = inf
        else:
            new_registered = new_effective

        if old_effective != new_effective:
            self._effective_deadline = new_effective
            for child in self._linked_children:
                child._update_computed_deadlines()

        if old_registered != new_registered:
            self._registered_deadline = new_registered
            runner = GLOBAL_RUN_CONTEXT.runner
            if old_registered != inf:
                del runner.deadlines[old_registered, id(self)]
            if new_registered != inf:
                runner.deadlines[new_registered, id(self)] = self

    @property
    def deadline(self):
        """Read-write, :class:`float`. An absolute time on the current
        run's clock at which this scope will automatically become
        cancelled. You can adjust the deadline by modifying this
        attribute, e.g.::

           # I need a little more time!
           cancel_scope.deadline += 30

        Note that for efficiency, the core run loop only checks for
        expired deadlines every once in a while. This means that in
        certain cases there may be a short delay between when the clock
        says the deadline should have expired, and when checkpoints
        start raising :exc:`~trio.Cancelled`. This is a very obscure
        corner case that you're unlikely to notice, but we document it
        for completeness. (If this *does* cause problems for you, of
        course, then `we want to know!
        <https://github.com/python-trio/trio/issues>`__)

        Defaults to :data:`math.inf`, which means "no deadline", though
        this can be overridden by the ``deadline=`` argument to
        the :class:`~trio.CancelScope` constructor.
        """
        return self._deadline

    @deadline.setter
    def deadline(self, new_deadline):
        self._deadline = float(new_deadline)
        self._update_computed_deadlines()

    @property
    def shield(self):
        """Read-write, :class:`bool`, default :data:`False`. So long as
        this is set to :data:`True`, then the code inside this scope
        will not receive :exc:`~trio.Cancelled` exceptions from scopes
        that are outside this scope. They can still receive
        :exc:`~trio.Cancelled` exceptions from (1) this scope, or (2)
        scopes inside this scope. You can modify this attribute::

           with trio.CancelScope() as cancel_scope:
               cancel_scope.shield = True
               # This cannot be interrupted by any means short of
               # killing the process:
               await sleep(10)

               cancel_scope.shield = False
               # Now this can be cancelled normally:
               await sleep(10)

        Defaults to :data:`False`, though this can be overridden by the
        ``shield=`` argument to the :class:`~trio.CancelScope` constructor.
        """
        return self._shield

    @shield.setter
    def shield(self, new_value):
        self._set_shield("_shield", new_value)

    @property
    def shield_during_cleanup(self):
        """Read-write, :class:`bool`, default :data:`False`. Like
        :attr:`shield`, but instead of shielding this scope from outside
        cancellations indefinitely, it only shields until the outside
        cancellation's :attr:`grace_period` expires. This allows for
        :ref:`blocking cleanup <cleanup-with-grace-period>` whose maximum
        duration is specified externally.

        You can set both :attr:`shield` and
        :attr:`~CancelScope.shield_during_cleanup` independently, but
        :attr:`shield` offers strictly more protection, so if
        :attr:`shield` is :data:`True` then the value of
        :attr:`~CancelScope.shield_during_cleanup` doesn't matter.
        """
        return self._shield_during_cleanup

    @shield_during_cleanup.setter
    def shield_during_cleanup(self, new_value):
        self._set_shield("_shield_during_cleanup", new_value)

    @enable_ki_protection
    def _set_shield(self, attr, new_value):
        if not isinstance(new_value, bool):
            raise TypeError("shield must be a bool")
        setattr(self, "_" + attr, new_value)
        if not new_value:
            for task in self._tasks:
                task._attempt_delivery_of_any_pending_cancel()
        for child in self._linked_children:
            setattr(child, attr, new_value)

    @property
    def grace_period(self):
        """Read-write, :class:`float`, default :data:`None`. Specifies the
        duration (in seconds) after this scope becomes cancelled during
        which cancel scopes nested inside this one should be shielded
        from cancellation if their :attr:`~CancelScope.shield_during_cleanup`
        attribute is :data:`True`.

        The default of :data:`None` means to inherit the
        :attr:`effective_grace_period` from the cancel scope immediately
        enclosing this one. If it also has a :attr:`grace_period` of
        :data:`None`, it will ask its own immediate parent, and so
        on, falling back to the ``grace_period`` argument to
        :func:`trio.run` (default zero) if none of our enclosing
        cancel scopes specified a grace period.

        Changing the :attr:`grace_period` after a cancellation has
        occurred (that did not specify its own grace period) will affect
        the future time at which the cancellation proceeds into
        :func:`shield_during_cleanup` scopes, but will not re-protect
        them if the previous grace period already expired.
        """
        return self._grace_period

    @grace_period.setter
    @enable_ki_protection
    def grace_period(self, new_grace_period):
        if new_grace_period is not None:
            new_grace_period = float(new_grace_period)
            if new_grace_period < 0:
                raise ValueError("grace period must be >= 0")

        for child in self._linked_children:
            child.grace_period = new_grace_period

        self._grace_period = new_grace_period
        self._grace_period_updated()

    def _grace_period_updated(self):
        if self._scope_task is None:
            return

        # Changing the grace period affects deadlines for all scopes
        # that are in their cleanup window, nested within this scope,
        # and not nested within a deeper scope whose grace period isn't
        # changing.
        with ExitStack() as stack:
            if self.cancel_called and not self.cleanup_expired:
                stack.enter_context(self._might_change_effective_deadline())

            my_index = self._scope_task._cancel_stack.index(self)
            affected_scopes = set()
            for task in self._tasks:
                assert task._cancel_stack[my_index] is self
                for scope in task._cancel_stack[my_index + 1:]:
                    if scope in affected_scopes:
                        continue
                    if scope.grace_period is not None:
                        break
                    if scope.cancel_called and not scope.cleanup_expired:
                        stack.enter_context(
                            scope._might_change_effective_deadline()
                        )
                        affected_scopes.add(scope)

    @grace_period.deleter
    def grace_period(self):
        self.grace_period = None

    @property
    def effective_grace_period(self):
        """Read-only, :class:`float`. The grace period that would be used
        if this scope were cancelled right now. This has the same value
        as :attr:`grace_period` if :attr:`grace_period` is not :data:`None`,
        and otherwise reflects the :attr:`effective_grace_period` of the
        cancel scope that immediately encloses this one's ``with`` block.
        If the ``with`` block is not active and no :attr:`grace_period` was
        specified, the :attr:`effective_grace_period` is :data:`None`.
        """
        if self._grace_period is not None:
            return self._grace_period
        if self._scope_task is None:
            return None

        effective_grace_period = 0
        for scope in self._scope_task._cancel_stack:
            if scope._grace_period is not None:
                effective_grace_period = scope._grace_period
            if scope is self:
                break
        else:  # pragma: no cover
            raise AssertionError("cancel stack corrupted")
        return effective_grace_period

    @property
    def _local_effective_grace_period(self):
        if self._local_grace_period is not None:
            return self._local_grace_period
        return self.effective_grace_period

    def _cancel_no_notify(self, as_of):
        # returns the affected tasks
        if not self.cancel_called:
            # Initial cancellation applies to us and all linked_children.
            with self._might_change_effective_deadline():
                self.cancel_called = True
                self._cleanup_started_at = as_of
                if self._local_effective_grace_period == 0:
                    # If we have no grace period, switch immediately
                    # to hard-cancel mode.
                    self.cleanup_expired = True

            affected_tasks = self._tasks
            if self._linked_children:
                affected_tasks = set(affected_tasks)
                for child in self._linked_children:
                    if not child.cancel_called:
                        affected_tasks.update(child._cancel_no_notify(as_of))

        elif not self.cleanup_expired:
            # Second cancellation represents expiration of the grace
            # period for cleanup. It applies only to this cancel
            # scope; our linked_children might have their own grace periods,
            # and our initial cancellation started those clocks
            # running.
            with self._might_change_effective_deadline():
                self.cleanup_expired = True
            affected_tasks = self._tasks

        else:  # pragma: no cover
            # An additional cancellation after we're already
            # hard-cancelled doesn't do anything further.
            # (We shouldn't actually be able to get here, because
            # the user-facing interfaces check cancel_called before
            # entering _cancel_no_notify, and the deadline manager
            # won't call us because we won't register a deadline
            # after cleanup_expired.)
            return set()

    @enable_ki_protection
    def cancel(self, *, grace_period=None):
        """Cancel the work in this scope.

        This method is idempotent, i.e., if the scope was already
        cancelled then this method silently does nothing (even if the
        previous cancellation used a different grace period). If the
        scope was not already cancelled, the cancellation proceeds
        as follows:

        * A grace period for this cancellation is determined. This is the
          value of the ``grace_period`` argument if it was specified,
          or the current :attr:`effective_grace_period` if not.

        * Work within this scope that is not within a nested
          :func:`shield_during_cleanup` scope is cancelled immediately.

        * Work within this scope that is within a nested
          :func:`shield_during_cleanup` scope is cancelled
          after the expiry of the grace period for this cancellation,
          or immediately if the grace period is zero.

        A grace period specified as an argument to :func:`cancel` will
        not be inherited by other cancel scopes and cannot be changed
        via assignment to the :attr:`CancelScope.grace_period` attribute.
        """
        if self.cancel_called:
            return

        if grace_period is not None:
            self._local_grace_period = grace_period
            for child in self.linked_children:
                if not child.cancel_called:
                    child._local_grace_period = grace_period

        for child in self._linked_children:
            child.cancel()
        self.cancel_called = True
        self._update_computed_deadlines()
        for task in self._tasks:
            task._attempt_delivery_of_any_pending_cancel()

    def _cancel_immediately_no_notify(self, as_of):
        if self.cleanup_expired:
            return set()

        with self._might_change_effective_deadline():
            if not self.cancel_called:
                self.cancel_called = True
                self._cleanup_started_at = as_of
            self.cleanup_expired = True
            self._local_grace_period = 0

        affected_tasks = self._tasks
        if self._linked_children:
            affected_tasks = set(affected_tasks)
            for child in self._linked_children:
                affected_tasks.update(
                    child._cancel_immediately_no_notify(as_of)
                )
        return affected_tasks

    @enable_ki_protection
    def cancel_immediately(self):
        """Cancel the work in this scope immediately, disregarding any grace
        period.

        If this scope is not yet cancelled, :meth:`cancel_immediately`
        behaves like ``cancel(grace_period=0)``. If the scope is
        already cancelled, it forces the immediate expiry of any
        grace period that might be outstanding.
        """
        for task in self._cancel_immediately_no_notify(_core.current_time()):
            task._attempt_delivery_of_any_pending_cancel()

    def _add_task(self, task):
        self._tasks.add(task)
        task._cancel_stack.append(self)

    def _remove_task(self, task):
        self._tasks.remove(task)
        try:
            assert task._cancel_stack[-1] is self
        except Exception as exc:
            import pdb; pdb.set_trace()
        task._cancel_stack.pop()

    # Used by the nursery.start trickiness
    def _tasks_removed_by_adoption(self, tasks):
        self._tasks.difference_update(tasks)
        self._update_computed_deadlines()  # in case we now have no tasks

    # Used by the nursery.start trickiness
    def _tasks_added_by_adoption(self, tasks):
        self._tasks.update(tasks)

    def _exc_filter(self, exc):
        if (
            isinstance(exc, Cancelled) and self.cancel_called
            and self._scope_task._pending_cancel_scope() is self
        ):
            self.cancelled_caught = True
            return None
        return exc

    def _close(self, exc):
        if exc is not None:
            exc = MultiError.filter(self._exc_filter, exc)
        self._remove_task(self._scope_task)
        self._update_computed_deadlines()  # we should now have no tasks
        self._scope_task = None
        return exc


@deprecated("0.10.0", issue=607, instead="trio.CancelScope")
def open_cancel_scope(*, deadline=inf, shield=False):
    """Returns a context manager which creates a new cancellation scope."""
    return CancelScope(deadline=deadline, shield=shield)


################################################################
# Nursery and friends
################################################################


# This code needs to be read alongside the code from Nursery.start to make
# sense.
@attr.s(cmp=False, hash=False, repr=False)
class _TaskStatus:
    _old_nursery = attr.ib()
    _new_nursery = attr.ib()
    _called_started = attr.ib(default=False)
    _value = attr.ib(default=None)

    def __repr__(self):
        return "<Task status object at {:#x}>".format(id(self))

    def started(self, value=None):
        if self._called_started:
            raise RuntimeError(
                "called 'started' twice on the same task status"
            )
        self._called_started = True
        self._value = value

        # If the old nursery is cancelled, then quietly quit now; the child
        # will eventually exit on its own, and we don't want to risk moving
        # children that might have propagating Cancelled exceptions into
        # a place with no cancelled cancel scopes to catch them.
        if _pending_cancel_scope(self._old_nursery._cancel_stack) is not None:
            return

        # Can't be closed, b/c we checked in start() and then _pending_starts
        # should keep it open.
        assert not self._new_nursery._closed

        # otherwise, find all the tasks under the old nursery, and move them
        # under the new nursery instead. This means:
        # - changing parents of direct children
        # - changing cancel stack of all direct+indirect children
        # - changing cancel stack of all direct+indirect children's nurseries
        # - checking for cancellation in all changed cancel stacks
        old_stack = self._old_nursery._cancel_stack
        new_stack = self._new_nursery._cancel_stack
        # LIFO todo stack for depth-first traversal
        todo = list(self._old_nursery._children)
        munged_tasks = []
        while todo:
            task = todo.pop()
            # Direct children need to be reparented
            if task._parent_nursery is self._old_nursery:
                self._old_nursery._children.remove(task)
                task._parent_nursery = self._new_nursery
                self._new_nursery._children.add(task)
            # Everyone needs their cancel scopes fixed up...
            assert task._cancel_stack[:len(old_stack)] == old_stack
            task._cancel_stack[:len(old_stack)] = new_stack
            # ...and their nurseries' cancel scopes fixed up.
            for nursery in task._child_nurseries:
                assert nursery._cancel_stack[:len(old_stack)] == old_stack
                nursery._cancel_stack[:len(old_stack)] = new_stack
                # And then add all the nursery's children to our todo list
                todo.extend(nursery._children)
            # And make a note to check for cancellation later
            munged_tasks.append(task)

        # That should have removed all the children from the old nursery
        assert not self._old_nursery._children

        # Tell all the cancel scopes about the change. (There are probably
        # some scopes in common between the two stacks, so some scopes will
        # get the same tasks removed and then immediately re-added. This is
        # fine though.)
        for cancel_scope in old_stack:
            cancel_scope._tasks_removed_by_adoption(munged_tasks)
        for cancel_scope in new_stack:
            cancel_scope._tasks_added_by_adoption(munged_tasks)

        # Moving child tasks' cancel scopes might have changed their effective
        # grace periods -- poke them to update. This can change deadlines but
        # doesn't deliver any cancellations.
        if (
            new_stack[-1].effective_grace_period !=
            old_stack[-1].effective_grace_period
        ):
            new_stack[-1]._grace_period_updated()

        # After all the delicate surgery is done, check for cancellation in
        # all the tasks that had their cancel scopes munged. This can trigger
        # arbitrary abort() callbacks, so we put it off until our internal
        # data structures are all self-consistent again.
        for task in munged_tasks:
            task._attempt_delivery_of_any_pending_cancel()

        # And finally, poke the old nursery so it notices that all its
        # children have disappeared and can exit.
        self._old_nursery._check_nursery_closed()


class NurseryManager:
    """Nursery context manager.

    Note we explicitly avoid @asynccontextmanager and @async_generator
    since they add a lot of extraneous stack frames to exceptions, as
    well as cause problematic behavior with handling of StopIteration
    and StopAsyncIteration.

    """

    @enable_ki_protection
    async def __aenter__(self):
        self._scope = CancelScope()
        self._scope.__enter__()
        self._nursery = Nursery(current_task(), self._scope)
        return self._nursery

    @enable_ki_protection
    async def __aexit__(self, etype, exc, tb):
        new_exc = await self._nursery._nested_child_finished(exc)
        # Tracebacks show the 'raise' line below out of context, so let's give
        # this variable a name that makes sense out of context.
        combined_error_from_nursery = self._scope._close(new_exc)
        if combined_error_from_nursery is None:
            return True
        elif combined_error_from_nursery is exc:
            return False
        else:
            # Copied verbatim from MultiErrorCatcher.  Python doesn't
            # allow us to encapsulate this __context__ fixup.
            old_context = combined_error_from_nursery.__context__
            try:
                raise combined_error_from_nursery
            finally:
                _, value, _ = sys.exc_info()
                assert value is combined_error_from_nursery
                value.__context__ = old_context

    def __enter__(self):
        raise RuntimeError(
            "use 'async with open_nursery(...)', not 'with open_nursery(...)'"
        )

    def __exit__(self):  # pragma: no cover
        assert False, """Never called, but should be defined"""


def open_nursery():
    """Returns an async context manager which must be used to create a
    new ``Nursery``.

    It does not block on entry; on exit it blocks until all child tasks
    have exited.

    """
    return NurseryManager()


class Nursery:
    def __init__(self, parent_task, cancel_scope):
        self._parent_task = parent_task
        parent_task._child_nurseries.append(self)
        # the cancel stack that children inherit - we take a snapshot, so it
        # won't be affected by any changes in the parent.
        self._cancel_stack = list(parent_task._cancel_stack)
        # the cancel scope that directly surrounds us; used for cancelling all
        # children.
        self.cancel_scope = cancel_scope
        assert self.cancel_scope is self._cancel_stack[-1]
        self._children = set()
        self._pending_excs = []
        # The "nested child" is how this code refers to the contents of the
        # nursery's 'async with' block, which acts like a child Task in all
        # the ways we can make it.
        self._nested_child_running = True
        self._parent_waiting_in_aexit = False
        self._pending_starts = 0
        self._closed = False

    @property
    def child_tasks(self):
        return frozenset(self._children)

    @property
    def parent_task(self):
        return self._parent_task

    def _add_exc(self, exc):
        self._pending_excs.append(exc)
        if isinstance(exc, _core.Cancelled):
            # A cancellation that propagates out of a task must be
            # associated with some cancel scope in the nursery's cancel
            # stack. If that scope is cancelled, it's cancelled for all
            # tasks in the nursery, and adding our own cancellation on
            # top of that (with a potentially different grace period)
            # just confuses things.
            pass
        else:
            self.cancel_scope.cancel()

    def _check_nursery_closed(self):
        if not any(
            [self._nested_child_running, self._children, self._pending_starts]
        ):
            self._closed = True
            if self._parent_waiting_in_aexit:
                self._parent_waiting_in_aexit = False
                GLOBAL_RUN_CONTEXT.runner.reschedule(self._parent_task)

    def _child_finished(self, task, outcome):
        self._children.remove(task)
        if type(outcome) is Error:
            self._add_exc(outcome.error)
        self._check_nursery_closed()

    async def _nested_child_finished(self, nested_child_exc):
        """Returns MultiError instance if there are pending exceptions."""
        if nested_child_exc is not None:
            self._add_exc(nested_child_exc)
        self._nested_child_running = False
        self._check_nursery_closed()

        if not self._closed:
            # If we get cancelled (or have an exception injected, like
            # KeyboardInterrupt), then save that, but still wait until our
            # children finish.
            def aborted(raise_cancel):
                self._add_exc(capture(raise_cancel).error)
                return Abort.FAILED

            self._parent_waiting_in_aexit = True
            await wait_task_rescheduled(aborted)
        else:
            # Nothing to wait for, so just execute a checkpoint -- but we
            # still need to mix any exception (e.g. from an external
            # cancellation) in with the rest of our exceptions.
            try:
                await checkpoint()
            except BaseException as exc:
                self._add_exc(exc)

        popped = self._parent_task._child_nurseries.pop()
        assert popped is self
        if self._pending_excs:
            return MultiError(self._pending_excs)

    def start_soon(self, async_fn, *args, name=None):
        GLOBAL_RUN_CONTEXT.runner.spawn_impl(async_fn, args, self, name)

    async def start(self, async_fn, *args, name=None):
        if self._closed:
            raise RuntimeError("Nursery is closed to new arrivals")
        try:
            self._pending_starts += 1
            async with open_nursery() as old_nursery:
                task_status = _TaskStatus(old_nursery, self)
                thunk = functools.partial(async_fn, task_status=task_status)
                old_nursery.start_soon(thunk, *args, name=name)
                # Wait for either _TaskStatus.started or an exception to
                # cancel this nursery:
            # If we get here, then the child either got reparented or exited
            # normally. The complicated logic is all in _TaskStatus.started().
            # (Any exceptions propagate directly out of the above.)
            if not task_status._called_started:
                raise RuntimeError(
                    "child exited without calling task_status.started()"
                )
            return task_status._value
        finally:
            self._pending_starts -= 1
            self._check_nursery_closed()

    def __del__(self):
        assert not self._children


################################################################
# Task and friends
################################################################


def _pending_cancel_scope(cancel_stack):
    # Return the outermost of the following two possibilities:
    # - the outermost cancel scope in cleanup state (cancel_called and not
    #   cleanup_expired) that is not outside a shield_during_cleanup
    # - the outermost cancel scope in fully-cancelled state
    #   (cleanup_expired, which implies also cancel_called)
    #   that is not outside any kind of shield

    pending_scope = None
    pending_with_cleanup_expired = None

    for scope in cancel_stack:
        # Check shielding before cancellation state, because shield
        # should not block processing of *this* scope's exception
        if scope._shield:
            # Full shield: nothing outside this scope can affect a task
            # that is currently executing code inside the scope.
            pending_scope = None
            pending_with_cleanup_expired = None

        if scope._shield_during_cleanup:
            # Shield during cleanup: only cancel scopes with cleanup_expired
            # outside this scope can affect a task that's executing code
            # inside it.
            pending_scope = pending_with_cleanup_expired

        if pending_scope is None and scope.cancel_called:
            pending_scope = scope

        if pending_with_cleanup_expired is None and scope.cleanup_expired:
            pending_with_cleanup_expired = scope

    return pending_scope


@attr.s(cmp=False, hash=False, repr=False)
class Task:
    _parent_nursery = attr.ib()
    coro = attr.ib()
    _runner = attr.ib()
    name = attr.ib()
    # PEP 567 contextvars context
    context = attr.ib()
    _counter = attr.ib(init=False, factory=itertools.count().__next__)

    # Invariant:
    # - for unscheduled tasks, _next_send is None
    # - for scheduled tasks, _next_send is an Outcome object,
    #   and custom_sleep_data is None
    # Tasks start out unscheduled.
    _next_send = attr.ib(default=None)
    _abort_func = attr.ib(default=None)
    custom_sleep_data = attr.ib(default=None)

    # For introspection and nursery.start()
    _child_nurseries = attr.ib(default=attr.Factory(list))

    # these are counts of how many cancel/schedule points this task has
    # executed, for assert{_no,}_yields
    # XX maybe these should be exposed as part of a statistics() method?
    _cancel_points = attr.ib(default=0)
    _schedule_points = attr.ib(default=0)

    def __repr__(self):
        return ("<Task {!r} at {:#x}>".format(self.name, id(self)))

    @property
    def parent_nursery(self):
        """The nursery this task is inside (or None if this is the "init"
        task).

        Example use case: drawing a visualization of the task tree in a
        debugger.

        """
        return self._parent_nursery

    @property
    def child_nurseries(self):
        """The nurseries this task contains.

        This is a list, with outer nurseries before inner nurseries.

        """
        return list(self._child_nurseries)

    ################
    # Cancellation
    ################

    _cancel_stack = attr.ib(default=attr.Factory(list), repr=False)

    def _pending_cancel_scope(self):
        return _pending_cancel_scope(self._cancel_stack)

    def _attempt_abort(self, raise_cancel):
        # Either the abort succeeds, in which case we will reschedule the
        # task, or else it fails, in which case it will worry about
        # rescheduling itself (hopefully eventually calling reraise to raise
        # the given exception, but not necessarily).
        success = self._abort_func(raise_cancel)
        if type(success) is not Abort:
            raise TrioInternalError("abort function must return Abort enum")
        # We only attempt to abort once per blocking call, regardless of
        # whether we succeeded or failed.
        self._abort_func = None
        if success is Abort.SUCCEEDED:
            self._runner.reschedule(self, capture(raise_cancel))

    def _attempt_delivery_of_any_pending_cancel(self):
        if self._abort_func is None:
            return
        if self._pending_cancel_scope() is None:
            return

        def raise_cancel():
            raise Cancelled._init()

        self._attempt_abort(raise_cancel)

    def _attempt_delivery_of_pending_ki(self):
        assert self._runner.ki_pending
        if self._abort_func is None:
            return

        def raise_cancel():
            self._runner.ki_pending = False
            raise KeyboardInterrupt

        self._attempt_abort(raise_cancel)


################################################################
# The central Runner object
################################################################


@attr.s(frozen=True)
class _RunStatistics:
    tasks_living = attr.ib()
    tasks_runnable = attr.ib()
    seconds_to_next_deadline = attr.ib()
    io_statistics = attr.ib()
    run_sync_soon_queue_size = attr.ib()


@attr.s(cmp=False, hash=False)
class Runner:
    clock = attr.ib()
    instruments = attr.ib()
    io_manager = attr.ib()

    # Run-local values, see _local.py
    _locals = attr.ib(default=attr.Factory(dict))

    runq = attr.ib(default=attr.Factory(deque))
    tasks = attr.ib(default=attr.Factory(set))

    # {(deadline, id(CancelScope)): CancelScope}
    # only contains scopes with non-infinite deadlines that are currently
    # attached to at least one task
    deadlines = attr.ib(default=attr.Factory(SortedDict))

    init_task = attr.ib(default=None)
    system_nursery = attr.ib(default=None)
    system_context = attr.ib(default=None)
    main_task = attr.ib(default=None)
    main_task_outcome = attr.ib(default=None)

    entry_queue = attr.ib(default=attr.Factory(EntryQueue))
    trio_token = attr.ib(default=None)

    _NO_SEND = object()

    def close(self):
        self.io_manager.close()
        self.entry_queue.close()
        if self.instruments:
            self.instrument("after_run")

    # Methods marked with @_public get converted into functions exported by
    # trio.hazmat:
    @_public
    def current_statistics(self):
        """Returns an object containing run-loop-level debugging information.

        Currently the following fields are defined:

        * ``tasks_living`` (int): The number of tasks that have been spawned
          and not yet exited.
        * ``tasks_runnable`` (int): The number of tasks that are currently
          queued on the run queue (as opposed to blocked waiting for something
          to happen).
        * ``seconds_to_next_deadline`` (float): The time until the next
          pending cancel scope deadline. May be negative if the deadline has
          expired but we haven't yet processed cancellations. May be
          :data:`~math.inf` if there are no pending deadlines.
        * ``run_sync_soon_queue_size`` (int): The number of
          unprocessed callbacks queued via
          :meth:`trio.hazmat.TrioToken.run_sync_soon`.
        * ``io_statistics`` (object): Some statistics from trio's I/O
          backend. This always has an attribute ``backend`` which is a string
          naming which operating-system-specific I/O backend is in use; the
          other attributes vary between backends.

        """
        if self.deadlines:
            next_deadline, _ = self.deadlines.keys()[0]
            seconds_to_next_deadline = next_deadline - self.current_time()
        else:
            seconds_to_next_deadline = float("inf")
        return _RunStatistics(
            tasks_living=len(self.tasks),
            tasks_runnable=len(self.runq),
            seconds_to_next_deadline=seconds_to_next_deadline,
            io_statistics=self.io_manager.statistics(),
            run_sync_soon_queue_size=self.entry_queue.size(),
        )

    @_public
    def current_time(self):
        """Returns the current time according to trio's internal clock.

        Returns:
            float: The current time.

        Raises:
            RuntimeError: if not inside a call to :func:`trio.run`.

        """
        return self.clock.current_time()

    @_public
    def current_clock(self):
        """Returns the current :class:`~trio.abc.Clock`.

        """
        return self.clock

    @_public
    def current_root_task(self):
        """Returns the current root :class:`Task`.

        This is the task that is the ultimate parent of all other tasks.

        """
        return self.init_task

    ################
    # Core task handling primitives
    ################

    @_public
    def reschedule(self, task, next_send=_NO_SEND):
        """Reschedule the given task with the given
        :class:`outcome.Outcome`.

        See :func:`wait_task_rescheduled` for the gory details.

        There must be exactly one call to :func:`reschedule` for every call to
        :func:`wait_task_rescheduled`. (And when counting, keep in mind that
        returning :data:`Abort.SUCCEEDED` from an abort callback is equivalent
        to calling :func:`reschedule` once.)

        Args:
          task (trio.hazmat.Task): the task to be rescheduled. Must be blocked
            in a call to :func:`wait_task_rescheduled`.
          next_send (outcome.Outcome): the value (or error) to return (or
            raise) from :func:`wait_task_rescheduled`.

        """
        if next_send is self._NO_SEND:
            next_send = Value(None)

        assert task._runner is self
        assert task._next_send is None
        task._next_send = next_send
        task._abort_func = None
        task.custom_sleep_data = None
        self.runq.append(task)
        if self.instruments:
            self.instrument("task_scheduled", task)

    def spawn_impl(self, async_fn, args, nursery, name, *, system_task=False):

        ######
        # Make sure the nursery is in working order
        ######

        # This sorta feels like it should be a method on nursery, except it
        # has to handle nursery=None for init. And it touches the internals of
        # all kinds of objects.
        if nursery is not None and nursery._closed:
            raise RuntimeError("Nursery is closed to new arrivals")
        if nursery is None:
            assert self.init_task is None

        ######
        # Call the function and get the coroutine object, while giving helpful
        # errors for common mistakes.
        ######

        def _return_value_looks_like_wrong_library(value):
            # Returned by legacy @asyncio.coroutine functions, which includes
            # a surprising proportion of asyncio builtins.
            if isinstance(value, collections.abc.Generator):
                return True
            # The protocol for detecting an asyncio Future-like object
            if getattr(value, "_asyncio_future_blocking", None) is not None:
                return True
            # asyncio.Future doesn't have _asyncio_future_blocking until
            # 3.5.3. We don't want to import asyncio, but this janky check
            # should work well enough for our purposes. And it also catches
            # tornado Futures and twisted Deferreds. By the time we're calling
            # this function, we already know something has gone wrong, so a
            # heuristic is pretty safe.
            if value.__class__.__name__ in ("Future", "Deferred"):
                return True
            return False

        try:
            coro = async_fn(*args)
        except TypeError:
            # Give good error for: nursery.start_soon(trio.sleep(1))
            if isinstance(async_fn, collections.abc.Coroutine):
                raise TypeError(
                    "trio was expecting an async function, but instead it got "
                    "a coroutine object {async_fn!r}\n"
                    "\n"
                    "Probably you did something like:\n"
                    "\n"
                    "  trio.run({async_fn.__name__}(...))            # incorrect!\n"
                    "  nursery.start_soon({async_fn.__name__}(...))  # incorrect!\n"
                    "\n"
                    "Instead, you want (notice the parentheses!):\n"
                    "\n"
                    "  trio.run({async_fn.__name__}, ...)            # correct!\n"
                    "  nursery.start_soon({async_fn.__name__}, ...)  # correct!"
                    .format(async_fn=async_fn)
                ) from None

            # Give good error for: nursery.start_soon(future)
            if _return_value_looks_like_wrong_library(async_fn):
                raise TypeError(
                    "trio was expecting an async function, but instead it got "
                    "{!r} – are you trying to use a library written for "
                    "asyncio/twisted/tornado or similar? That won't work "
                    "without some sort of compatibility shim."
                    .format(async_fn)
                ) from None

            raise

        # We can't check iscoroutinefunction(async_fn), because that will fail
        # for things like functools.partial objects wrapping an async
        # function. So we have to just call it and then check whether the
        # return value is a coroutine object.
        if not isinstance(coro, collections.abc.Coroutine):
            # Give good error for: nursery.start_soon(func_returning_future)
            if _return_value_looks_like_wrong_library(coro):
                raise TypeError(
                    "start_soon got unexpected {!r} – are you trying to use a "
                    "library written for asyncio/twisted/tornado or similar? "
                    "That won't work without some sort of compatibility shim."
                    .format(coro)
                )

            if isasyncgen(coro):
                raise TypeError(
                    "start_soon expected an async function but got an async "
                    "generator {!r}".format(coro)
                )

            # Give good error for: nursery.start_soon(some_sync_fn)
            raise TypeError(
                "trio expected an async function, but {!r} appears to be "
                "synchronous".format(
                    getattr(async_fn, "__qualname__", async_fn)
                )
            )

        ######
        # Set up the Task object
        ######

        if name is None:
            name = async_fn
        if isinstance(name, functools.partial):
            name = name.func
        if not isinstance(name, str):
            try:
                name = "{}.{}".format(name.__module__, name.__qualname__)
            except AttributeError:
                name = repr(name)

        if system_task:
            context = self.system_context.copy()
        else:
            context = copy_context()

        task = Task(
            coro=coro,
            parent_nursery=nursery,
            runner=self,
            name=name,
            context=context,
        )
        self.tasks.add(task)
        coro.cr_frame.f_locals.setdefault(
            LOCALS_KEY_KI_PROTECTION_ENABLED, system_task
        )

        if nursery is not None:
            nursery._children.add(task)
            for scope in nursery._cancel_stack:
                scope._add_task(task)

        if self.instruments:
            self.instrument("task_spawned", task)
        # Special case: normally next_send should be an Outcome, but for the
        # very first send we have to send a literal unboxed None.
        self.reschedule(task, None)
        return task

    def task_exited(self, task, outcome):
        while task._cancel_stack:
            task._cancel_stack[-1]._remove_task(task)
        self.tasks.remove(task)
        if task is self.main_task:
            self.main_task_outcome = outcome
            self.system_nursery.cancel_scope.cancel()
            self.system_nursery._child_finished(task, Value(None))
        elif task is self.init_task:
            # If the init task crashed, then something is very wrong and we
            # let the error propagate. (It'll eventually be wrapped in a
            # TrioInternalError.)
            outcome.unwrap()
            # the init task should be the last task to exit. If not, then
            # something is very wrong.
            if self.tasks:  # pragma: no cover
                raise TrioInternalError
        else:
            task._parent_nursery._child_finished(task, outcome)

        if self.instruments:
            self.instrument("task_exited", task)

    ################
    # System tasks and init
    ################

    @_public
    def spawn_system_task(self, async_fn, *args, name=None):
        """Spawn a "system" task.

        System tasks have a few differences from regular tasks:

        * They don't need an explicit nursery; instead they go into the
          internal "system nursery".

        * If a system task raises an exception, then it's converted into a
          :exc:`~trio.TrioInternalError` and *all* tasks are cancelled. If you
          write a system task, you should be careful to make sure it doesn't
          crash.

        * System tasks are automatically cancelled when the main task exits.

        * By default, system tasks have :exc:`KeyboardInterrupt` protection
          *enabled*. If you want your task to be interruptible by control-C,
          then you need to use :func:`disable_ki_protection` explicitly (and
          come up with some plan for what to do with a
          :exc:`KeyboardInterrupt`, given that system tasks aren't allowed to
          raise exceptions).

        * System tasks do not inherit context variables from their creator.

        Args:
          async_fn: An async callable.
          args: Positional arguments for ``async_fn``. If you want to pass
              keyword arguments, use :func:`functools.partial`.
          name: The name for this task. Only used for debugging/introspection
              (e.g. ``repr(task_obj)``). If this isn't a string,
              :func:`spawn_system_task` will try to make it one. A common use
              case is if you're wrapping a function before spawning a new
              task, you might pass the original function as the ``name=`` to
              make debugging easier.

        Returns:
          Task: the newly spawned task

        """

        return self.spawn_impl(
            async_fn, args, self.system_nursery, name, system_task=True
        )

    async def init(self, async_fn, args, grace_period):
        async with open_nursery() as system_nursery:
            self.system_nursery = system_nursery
            try:
                system_nursery.cancel_scope.grace_period = grace_period
                self.main_task = self.spawn_impl(
                    async_fn, args, system_nursery, None
                )
            except BaseException as exc:
                self.main_task_outcome = Error(exc)
                system_nursery.cancel_scope.cancel()
            self.entry_queue.spawn()

    ################
    # Outside context problems
    ################

    @_public
    def current_trio_token(self):
        """Retrieve the :class:`TrioToken` for the current call to
        :func:`trio.run`.

        """
        if self.trio_token is None:
            self.trio_token = TrioToken(self.entry_queue)
        return self.trio_token

    ################
    # KI handling
    ################

    ki_pending = attr.ib(default=False)

    # deliver_ki is broke. Maybe move all the actual logic and state into
    # RunToken, and we'll only have one instance per runner? But then we can't
    # have a public constructor. Eh, but current_run_token() returning a
    # unique object per run feels pretty nice. Maybe let's just go for it. And
    # keep the class public so people can isinstance() it if they want.

    # This gets called from signal context
    def deliver_ki(self):
        self.ki_pending = True
        try:
            self.entry_queue.run_sync_soon(self._deliver_ki_cb)
        except RunFinishedError:
            pass

    def _deliver_ki_cb(self):
        if not self.ki_pending:
            return
        # Can't happen because main_task and run_sync_soon_task are created at
        # the same time -- so even if KI arrives before main_task is created,
        # we won't get here until afterwards.
        assert self.main_task is not None
        if self.main_task_outcome is not None:
            # We're already in the process of exiting -- leave ki_pending set
            # and we'll check it again on our way out of run().
            return
        self.main_task._attempt_delivery_of_pending_ki()

    ################
    # Quiescing
    ################

    waiting_for_idle = attr.ib(default=attr.Factory(SortedDict))

    @_public
    async def wait_all_tasks_blocked(self, cushion=0.0, tiebreaker=0):
        """Block until there are no runnable tasks.

        This is useful in testing code when you want to give other tasks a
        chance to "settle down". The calling task is blocked, and doesn't wake
        up until all other tasks are also blocked for at least ``cushion``
        seconds. (Setting a non-zero ``cushion`` is intended to handle cases
        like two tasks talking to each other over a local socket, where we
        want to ignore the potential brief moment between a send and receive
        when all tasks are blocked.)

        Note that ``cushion`` is measured in *real* time, not the trio clock
        time.

        If there are multiple tasks blocked in :func:`wait_all_tasks_blocked`,
        then the one with the shortest ``cushion`` is the one woken (and
        this task becoming unblocked resets the timers for the remaining
        tasks). If there are multiple tasks that have exactly the same
        ``cushion``, then the one with the lowest ``tiebreaker`` value is
        woken first. And if there are multiple tasks with the same ``cushion``
        and the same ``tiebreaker``, then all are woken.

        You should also consider :class:`trio.testing.Sequencer`, which
        provides a more explicit way to control execution ordering within a
        test, and will often produce more readable tests.

        Example:
          Here's an example of one way to test that trio's locks are fair: we
          take the lock in the parent, start a child, wait for the child to be
          blocked waiting for the lock (!), and then check that we can't
          release and immediately re-acquire the lock::

             async def lock_taker(lock):
                 await lock.acquire()
                 lock.release()

             async def test_lock_fairness():
                 lock = trio.Lock()
                 await lock.acquire()
                 async with trio.open_nursery() as nursery:
                     child = nursery.start_soon(lock_taker, lock)
                     # child hasn't run yet, we have the lock
                     assert lock.locked()
                     assert lock._owner is trio.current_task()
                     await trio.testing.wait_all_tasks_blocked()
                     # now the child has run and is blocked on lock.acquire(), we
                     # still have the lock
                     assert lock.locked()
                     assert lock._owner is trio.current_task()
                     lock.release()
                     try:
                         # The child has a prior claim, so we can't have it
                         lock.acquire_nowait()
                     except trio.WouldBlock:
                         assert lock._owner is child
                         print("PASS")
                     else:
                         print("FAIL")

        """
        task = current_task()
        key = (cushion, tiebreaker, id(task))
        self.waiting_for_idle[key] = task

        def abort(_):
            del self.waiting_for_idle[key]
            return Abort.SUCCEEDED

        await wait_task_rescheduled(abort)

    ################
    # Instrumentation
    ################

    def instrument(self, method_name, *args):
        if not self.instruments:
            return

        for instrument in list(self.instruments):
            try:
                method = getattr(instrument, method_name)
            except AttributeError:
                continue
            try:
                method(*args)
            except:
                self.instruments.remove(instrument)
                INSTRUMENT_LOGGER.exception(
                    "Exception raised when calling %r on instrument %r. "
                    "Instrument has been disabled.", method_name, instrument
                )

    @_public
    def add_instrument(self, instrument):
        """Start instrumenting the current run loop with the given instrument.

        Args:
          instrument (trio.abc.Instrument): The instrument to activate.

        If ``instrument`` is already active, does nothing.

        """
        if instrument not in self.instruments:
            self.instruments.append(instrument)

    @_public
    def remove_instrument(self, instrument):
        """Stop instrumenting the current run loop with the given instrument.

        Args:
          instrument (trio.abc.Instrument): The instrument to de-activate.

        Raises:
          KeyError: if the instrument is not currently active. This could
              occur either because you never added it, or because you added it
              and then it raised an unhandled exception and was automatically
              deactivated.

        """
        # We're moving 'instruments' to being a set, so raise KeyError like
        # set.remove does.
        try:
            self.instruments.remove(instrument)
        except ValueError as exc:
            raise KeyError(*exc.args)


################################################################
# run
################################################################


def run(
    async_fn,
    *args,
    clock=None,
    instruments=(),
    restrict_keyboard_interrupt_to_checkpoints=False,
    grace_period=0
):
    """Run a trio-flavored async function, and return the result.

    Calling::

       run(async_fn, *args)

    is the equivalent of::

       await async_fn(*args)

    except that :func:`run` can (and must) be called from a synchronous
    context.

    This is trio's main entry point. Almost every other function in trio
    requires that you be inside a call to :func:`run`.

    Args:
      async_fn: An async function.

      args: Positional arguments to be passed to *async_fn*. If you need to
          pass keyword arguments, then use :func:`functools.partial`.

      clock: ``None`` to use the default system-specific monotonic clock;
          otherwise, an object implementing the :class:`trio.abc.Clock`
          interface, like (for example) a :class:`trio.testing.MockClock`
          instance.

      instruments (list of :class:`trio.abc.Instrument` objects): Any
          instrumentation you want to apply to this run. This can also be
          modified during the run; see :ref:`instrumentation`.

      restrict_keyboard_interrupt_to_checkpoints (bool): What happens if the
          user hits control-C while :func:`run` is running? If this argument
          is False (the default), then you get the standard Python behavior: a
          :exc:`KeyboardInterrupt` exception will immediately interrupt
          whatever task is running (or if no task is running, then trio will
          wake up a task to be interrupted). Alternatively, if you set this
          argument to True, then :exc:`KeyboardInterrupt` delivery will be
          delayed: it will be *only* be raised at :ref:`checkpoints
          <checkpoints>`, like a :exc:`Cancelled` exception.

          The default behavior is nice because it means that even if you
          accidentally write an infinite loop that never executes any
          checkpoints, then you can still break out of it using control-C.
          The alternative behavior is nice if you're paranoid about a
          :exc:`KeyboardInterrupt` at just the wrong place leaving your
          program in an inconsistent state, because it means that you only
          have to worry about :exc:`KeyboardInterrupt` at the exact same
          places where you already have to worry about :exc:`Cancelled`.

          This setting has no effect if your program has registered a custom
          SIGINT handler, or if :func:`run` is called from anywhere but the
          main thread (this is a Python limitation), or if you use
          :func:`open_signal_receiver` to catch SIGINT.

      grace_period (float): The :ref:`grace period
          <cleanup-with-grace-period>`, in seconds, to use for
          cancellations where no enclosing scope specifies one.
          Default is zero (no grace period). Effectively this behaves
          as if the body of ``async_fn``, and every system task,
          were enclosed in a ``with
          trio.CancelScope(grace_period=...):`` block.

    Returns:
      Whatever ``async_fn`` returns.

    Raises:
      TrioInternalError: if an unexpected error is encountered inside trio's
          internal machinery. This is a bug and you should `let us know
          <https://github.com/python-trio/trio/issues>`__.

      Anything else: if ``async_fn`` raises an exception, then :func:`run`
          propagates it.

    """

    __tracebackhide__ = True

    # Do error-checking up front, before we enter the TrioInternalError
    # try/catch
    #
    # It wouldn't be *hard* to support nested calls to run(), but I can't
    # think of a single good reason for it, so let's be conservative for
    # now:
    if hasattr(GLOBAL_RUN_CONTEXT, "runner"):
        raise RuntimeError("Attempted to call run() from inside a run()")

    if clock is None:
        clock = SystemClock()
    instruments = list(instruments)
    io_manager = TheIOManager()
    system_context = copy_context()
    system_context.run(current_async_library_cvar.set, "trio")
    runner = Runner(
        clock=clock,
        instruments=instruments,
        io_manager=io_manager,
        system_context=system_context,
    )
    GLOBAL_RUN_CONTEXT.runner = runner
    locals()[LOCALS_KEY_KI_PROTECTION_ENABLED] = True

    # KI handling goes outside the core try/except/finally to avoid a window
    # where KeyboardInterrupt would be allowed and converted into an
    # TrioInternalError:
    try:
        with ki_manager(
            runner.deliver_ki, restrict_keyboard_interrupt_to_checkpoints
        ):
            try:
                with closing(runner):
                    # The main reason this is split off into its own function
                    # is just to get rid of this extra indentation.
                    run_impl(runner, async_fn, args, grace_period)
            except TrioInternalError:
                raise
            except BaseException as exc:
                raise TrioInternalError(
                    "internal error in trio - please file a bug!"
                ) from exc
            finally:
                GLOBAL_RUN_CONTEXT.__dict__.clear()
            # Inlined copy of runner.main_task_outcome.unwrap() to avoid
            # cluttering every single trio traceback with an extra frame.
            if type(runner.main_task_outcome) is Value:
                return runner.main_task_outcome.value
            else:
                raise runner.main_task_outcome.error
    finally:
        # To guarantee that we never swallow a KeyboardInterrupt, we have to
        # check for pending ones once more after leaving the context manager:
        if runner.ki_pending:
            # Implicitly chains with any exception from outcome.unwrap():
            raise KeyboardInterrupt


# 24 hours is arbitrary, but it avoids issues like people setting timeouts of
# 10**20 and then getting integer overflows in the underlying system calls.
_MAX_TIMEOUT = 24 * 60 * 60


def run_impl(runner, async_fn, args, grace_period):
    __tracebackhide__ = True

    if runner.instruments:
        runner.instrument("before_run")
    runner.clock.start_clock()
    runner.init_task = runner.spawn_impl(
        runner.init,
        (async_fn, args, grace_period),
        None,
        "<init>",
        system_task=True,
    )

    # You know how people talk about "event loops"? This 'while' loop right
    # here is our event loop:
    while runner.tasks:
        if runner.runq:
            timeout = 0
        elif runner.deadlines:
            deadline, _ = runner.deadlines.keys()[0]
            timeout = runner.clock.deadline_to_sleep_time(deadline)
        else:
            timeout = _MAX_TIMEOUT
        timeout = min(max(0, timeout), _MAX_TIMEOUT)

        idle_primed = False
        if runner.waiting_for_idle:
            cushion, tiebreaker, _ = runner.waiting_for_idle.keys()[0]
            if cushion < timeout:
                timeout = cushion
                idle_primed = True

        if runner.instruments:
            runner.instrument("before_io_wait", timeout)

        runner.io_manager.handle_io(timeout)

        if runner.instruments:
            runner.instrument("after_io_wait", timeout)

        # Process cancellations due to deadline expiry
        now = runner.clock.current_time()
        while runner.deadlines:
            (deadline, _), cancel_scope = runner.deadlines.peekitem(0)
            if deadline <= now:
                # This removes the given scope from runner.deadlines,
                # or reinserts it with a later deadline if it has a
                # grace period.
                cancel_scope.cancel(as_of=deadline)
                idle_primed = False
            else:
                break

        if not runner.runq and idle_primed:
            while runner.waiting_for_idle:
                key, task = runner.waiting_for_idle.peekitem(0)
                if key[:2] == (cushion, tiebreaker):
                    del runner.waiting_for_idle[key]
                    runner.reschedule(task)
                else:
                    break

        # Process all runnable tasks, but only the ones that are already
        # runnable now. Anything that becomes runnable during this cycle needs
        # to wait until the next pass. This avoids various starvation issues
        # by ensuring that there's never an unbounded delay between successive
        # checks for I/O.
        #
        # Also, we randomize the order of each batch to avoid assumptions
        # about scheduling order sneaking in. In the long run, I suspect we'll
        # either (a) use strict FIFO ordering and document that for
        # predictability/determinism, or (b) implement a more sophisticated
        # scheduler (e.g. some variant of fair queueing), for better behavior
        # under load. For now, this is the worst of both worlds - but it keeps
        # our options open. (If we do decide to go all in on deterministic
        # scheduling, then there are other things that will probably need to
        # change too, like the deadlines tie-breaker and the non-deterministic
        # ordering of task._notify_queues.)
        batch = list(runner.runq)
        if _ALLOW_DETERMINISTIC_SCHEDULING:
            # We're running under Hypothesis, and pytest-trio has patched this
            # in to make the scheduler deterministic and avoid flaky tests.
            # It's not worth the (small) performance cost in normal operation,
            # since we'll shuffle the list and _r is only seeded for tests.
            batch.sort(key=lambda t: t._counter)
        runner.runq.clear()
        _r.shuffle(batch)
        while batch:
            task = batch.pop()
            GLOBAL_RUN_CONTEXT.task = task

            if runner.instruments:
                runner.instrument("before_task_step", task)

            next_send = task._next_send
            task._next_send = None
            final_outcome = None
            try:
                # We used to unwrap the Outcome object here and send/throw its
                # contents in directly, but it turns out that .throw() is
                # buggy, at least on CPython 3.6 and earlier:
                #   https://bugs.python.org/issue29587
                #   https://bugs.python.org/issue29590
                # So now we send in the Outcome object and unwrap it on the
                # other side.
                msg = task.context.run(task.coro.send, next_send)
            except StopIteration as stop_iteration:
                final_outcome = Value(stop_iteration.value)
            except BaseException as task_exc:
                # Store for later, removing uninteresting top frames: 1 frame
                # we always remove, because it's this function catching it,
                # and then in addition we remove however many more Context.run
                # adds.
                tb = task_exc.__traceback__.tb_next
                for _ in range(CONTEXT_RUN_TB_FRAMES):
                    tb = tb.tb_next
                final_outcome = Error(task_exc.with_traceback(tb))

            if final_outcome is not None:
                # We can't call this directly inside the except: blocks above,
                # because then the exceptions end up attaching themselves to
                # other exceptions as __context__ in unwanted ways.
                runner.task_exited(task, final_outcome)
            else:
                task._schedule_points += 1
                if msg is CancelShieldedCheckpoint:
                    runner.reschedule(task)
                elif type(msg) is WaitTaskRescheduled:
                    task._cancel_points += 1
                    task._abort_func = msg.abort_func
                    # KI is "outside" all cancel scopes, so check for it
                    # before checking for regular cancellation:
                    if runner.ki_pending and task is runner.main_task:
                        task._attempt_delivery_of_pending_ki()
                    task._attempt_delivery_of_any_pending_cancel()
                elif type(msg) is PermanentlyDetachCoroutineObject:
                    # Pretend the task just exited with the given outcome
                    runner.task_exited(task, msg.final_outcome)
                else:
                    exc = TypeError(
                        "trio.run received unrecognized yield message {!r}. "
                        "Are you trying to use a library written for some "
                        "other framework like asyncio? That won't work "
                        "without some kind of compatibility shim.".format(msg)
                    )
                    # How can we resume this task? It's blocked in code we
                    # don't control, waiting for some message that we know
                    # nothing about. We *could* try using coro.throw(...) to
                    # blast an exception in and hope that it propagates out,
                    # but (a) that's complicated because we aren't set up to
                    # resume a task via .throw(), and (b) even if we did,
                    # there's no guarantee that the foreign code will respond
                    # the way we're hoping. So instead we abandon this task
                    # and propagate the exception into the task's spawner.
                    runner.task_exited(task, Error(exc))

            if runner.instruments:
                runner.instrument("after_task_step", task)
            del GLOBAL_RUN_CONTEXT.task


################################################################
# Other public API functions
################################################################


class _TaskStatusIgnored:
    def __repr__(self):
        return "TASK_STATUS_IGNORED"

    def started(self, value=None):
        pass


TASK_STATUS_IGNORED = _TaskStatusIgnored()


def current_task():
    """Return the :class:`Task` object representing the current task.

    Returns:
      Task: the :class:`Task` that called :func:`current_task`.

    """

    try:
        return GLOBAL_RUN_CONTEXT.task
    except AttributeError:
        raise RuntimeError("must be called from async context") from None


def current_effective_deadline():
    """Returns the current effective deadline for the current task.

    This function examines all the cancellation scopes that are currently in
    effect (taking into account shielding), and returns the deadline that will
    expire first.

    One example of where this might be is useful is if your code is trying to
    decide whether to begin an expensive operation like an RPC call, but wants
    to skip it if it knows that it can't possibly complete in the available
    time. Another example would be if you're using a protocol like gRPC that
    `propagates timeout information to the remote peer
    <http://www.grpc.io/docs/guides/concepts.html#deadlines>`__; this function
    gives a way to fetch that information so you can send it along.

    If this is called in a context where a cancellation is currently active
    (i.e., a blocking call will immediately raise :exc:`Cancelled`), then
    returned deadline is ``-inf``. If it is called in a context where no
    scopes have a deadline set, it returns ``inf``.

    Returns:
        float: the effective deadline, as an absolute time.

    """
    task = current_task()
    deadline = inf
    cleanup_expiry = inf

    for scope in task._cancel_stack:
        if scope._shield:
            deadline = cleanup_expiry = inf
        if scope._shield_during_cleanup:
            deadline = cleanup_expiry

        if scope.cancel_called:
            deadline = -inf
            if scope.cleanup_expired:
                cleanup_expiry = -inf
            else:
                cleanup_expiry = min(
                    cleanup_expiry, scope._cleanup_started_at +
                    scope._local_effective_grace_period
                )
        else:
            deadline = min(deadline, scope._effective_deadline)
            cleanup_expiry = min(
                cleanup_expiry,
                scope._deadline + scope._local_effective_grace_period
            )

    return deadline


async def checkpoint():
    """A pure :ref:`checkpoint <checkpoints>`.

    This checks for cancellation and allows other tasks to be scheduled,
    without otherwise blocking.

    Note that the scheduler has the option of ignoring this and continuing to
    run the current task if it decides this is appropriate (e.g. for increased
    efficiency).

    Equivalent to ``await trio.sleep(0)`` (which is implemented by calling
    :func:`checkpoint`.)

    """
    with CancelScope(deadline=-inf):
        await _core.wait_task_rescheduled(lambda _: _core.Abort.SUCCEEDED)


async def checkpoint_if_cancelled():
    """Issue a :ref:`checkpoint <checkpoints>` if the calling context has been
    cancelled.

    Equivalent to (but potentially more efficient than)::

        if trio.current_effective_deadline() == -inf:
            await trio.hazmat.checkpoint()

    This is either a no-op, or else it allow other tasks to be scheduled and
    then raises :exc:`trio.Cancelled`.

    Typically used together with :func:`cancel_shielded_checkpoint`.

    """
    task = current_task()
    if (
        task._pending_cancel_scope() is not None or
        (task is task._runner.main_task and task._runner.ki_pending)
    ):
        await _core.checkpoint()
        assert False  # pragma: no cover
    task._cancel_points += 1


_WRAPPER_TEMPLATE = """
def wrapper(*args, **kwargs):
    locals()[LOCALS_KEY_KI_PROTECTION_ENABLED] = True
    try:
        meth = GLOBAL_RUN_CONTEXT.{}.{}
    except AttributeError:
        raise RuntimeError("must be called from async context") from None
    return meth(*args, **kwargs)
"""


def _generate_method_wrappers(cls, path_to_instance):
    for methname, fn in cls.__dict__.items():
        if callable(fn) and getattr(fn, "_public", False):
            # Create a wrapper function that looks up this method in the
            # current thread-local context version of this object, and calls
            # it. exec() is a bit ugly but the resulting code is faster and
            # simpler than doing some loop over getattr.
            ns = {
                "GLOBAL_RUN_CONTEXT":
                    GLOBAL_RUN_CONTEXT,
                "LOCALS_KEY_KI_PROTECTION_ENABLED":
                    LOCALS_KEY_KI_PROTECTION_ENABLED
            }
            exec(_WRAPPER_TEMPLATE.format(path_to_instance, methname), ns)
            wrapper = ns["wrapper"]
            # 'fn' is the *unbound* version of the method, but our exported
            # function has the same API as the *bound* version of the
            # method. So create a dummy bound method object:
            from types import MethodType
            bound_fn = MethodType(fn, object())
            # Then set exported function's metadata to match it:
            from functools import update_wrapper
            update_wrapper(wrapper, bound_fn)
            # And finally export it:
            globals()[methname] = wrapper
            __all__.append(methname)


_generate_method_wrappers(Runner, "runner")
_generate_method_wrappers(TheIOManager, "runner.io_manager")
