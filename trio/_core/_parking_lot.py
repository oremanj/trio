# ParkingLot provides an abstraction for a fair waitqueue with cancellation
# and requeueing support. Inspiration:
#
#    https://webkit.org/blog/6161/locking-in-webkit/
#    https://amanieu.github.io/parking_lot/
#
# which were in turn heavily influenced by
#
#    http://gee.cs.oswego.edu/dl/papers/aqs.pdf
#
# Compared to these, our use of cooperative scheduling allows some
# simplifications (no need for internal locking). On the other hand, the need
# to support trio's strong cancellation semantics adds some complications
# (tasks need to know where they're queued so they can cancel). Also, in the
# above work, the ParkingLot is a global structure that holds a collection of
# waitqueues keyed by lock address, and which are opportunistically allocated
# and destroyed as contention arises; this allows the worst-case memory usage
# for all waitqueues to be O(#tasks). Here we allocate a separate wait queue
# for each synchronization object, so we're O(#objects + #tasks). This isn't
# *so* bad since compared to our synchronization objects are heavier than
# theirs and our tasks are lighter, so for us #objects is smaller and #tasks
# is larger.
#
# This is in the core because for two reasons. First, it's used by
# UnboundedQueue, and UnboundedQueue is used for a number of things in the
# core. And second, it's responsible for providing fairness to all of our
# high-level synchronization primitives (locks, queues, etc.). For now with
# our FIFO scheduler this is relatively trivial (it's just a FIFO waitqueue),
# but in the future we ever start support task priorities or fair scheduling
#
#    https://github.com/python-trio/trio/issues/32
#
# then all we'll have to do is update this. (Well, full-fledged task
# priorities might also require priority inheritance, which would require more
# work.)
#
# For discussion of data structures to use here, see:
#
#     https://github.com/dabeaz/curio/issues/136
#
# (and also the articles above). Currently we use a SortedDict ordered by a
# global monotonic counter that ensures FIFO ordering. The main advantage of
# this is that it's easy to implement :-). An intrusive doubly-linked list
# would also be a natural approach, so long as we only handle FIFO ordering.
#
# XX: should we switch to the shared global ParkingLot approach?
#
# XX: if we do add WFQ, then we might have to drop the current feature where
# unpark returns the tasks that were unparked. Rationale: suppose that at the
# time we call unpark, the next task is deprioritized... and then, before it
# becomes runnable, a new task parks which *is* runnable. Ideally we should
# immediately wake the new task, and leave the old task on the queue for
# later. But this means we can't commit to which task we are unparking when
# unpark is called.
#
# See: https://github.com/python-trio/trio/issues/53

from itertools import count
import attr
from collections import OrderedDict

from .. import _core

__all__ = ["ParkingLot"]


@attr.s(frozen=True)
class _ParkingLotStatistics:
    tasks_waiting = attr.ib()


@attr.s(cmp=False, hash=False)
class ParkingLot:
    """A fair wait queue with cancellation and requeueing.

    This class encapsulates the tricky parts of implementing a wait
    queue. It's useful for implementing higher-level synchronization
    primitives like queues and locks.

    In addition to the methods below, you can use ``len(parking_lot)`` to get
    the number of parked tasks, and ``if parking_lot: ...`` to check whether
    there are any parked tasks.

    """

    # {task: cookie}
    _parked = attr.ib(default=attr.Factory(OrderedDict), init=False)

    def __len__(self):
        """Returns the number of parked tasks.

        """
        return len(self._parked)

    def __bool__(self):
        """True if there are parked tasks, False otherwise.

        """
        return bool(self._parked)

    def count_matching(self, condition):
        """Returns the number of tasks currently blocked in a call to
        ``park(cookie)`` for which ``condition(cookie)`` returns true.

        Args:
          condition: A function of one argument that takes a cookie
              that was passed to :meth:`park` and returns true if the
              task should be included in the returned count.
              Calls will be made in the order in which tasks would be
              woken up. The function must not make reentrant calls to
              methods of this :class:`ParkingLot`.

        Returns:
          The number of parked tasks whose cookies satisfy the condition.

        """
        return sum(1 for cookie in self._parked.values() if condition(cookie))

    # XX this currently returns None
    # if we ever add the ability to repark while one's resuming place in
    # line (for false wakeups), then we could have it return a ticket that
    # abstracts the "place in line" concept.
    @_core.enable_ki_protection
    async def park(self, cookie=None):
        """Park the current task until woken by a call to one of the ``unpark``
        methods.

        Args:
          cookie: An arbitrary object that denotes what the
              current task is waiting for. The :class:`ParkingLot` does not
              inspect this directly, but you can use it to decide which
              tasks should be woken by a call to :meth:`unpark_if`,
              :meth:`unpark_while`, or :meth:`unpark_matching`.
              For example, if you're implementing a readers-writer lock,
              this might indicate whether the current task wants to acquire
              the lock as a reader or as a writer.

        Returns:
          The ``value`` passed to :meth:`unpark`, or ``None`` if no value was
          specified or a different unparking mechanism was used.

        """
        task = _core.current_task()
        self._parked[task] = cookie
        task.custom_sleep_data = self

        def abort_fn(_):
            del task.custom_sleep_data._parked[task]
            return _core.Abort.SUCCEEDED

        return await _core.wait_task_rescheduled(abort_fn)

    # impound()?

    def _pop_several(self, count):
        for _ in range(min(count, len(self._parked))):
            yield self._parked.popitem(last=False)

    @_core.enable_ki_protection
    def unpark(self, *, count=1, value=None):
        """Unpark one or more tasks.

        This wakes up ``count`` tasks that are blocked in :meth:`park`. If
        there are fewer than ``count`` tasks parked, :meth:`unpark` wakes as
        many tasks as are available and then returns successfully.

        Args:
          count (int): The number of tasks to unpark.
          value (object): The value which should be returned from :meth:`park`
              in each unparked task.

        Returns:
          A list of the tasks that were woken up.

        """
        tasks = [task for task, cookie in self._pop_several(count)]
        for task in tasks:
            _core.reschedule(task, outcome.Value(value))
        return tasks

    def unpark_all(self):
        """Unpark all parked tasks.

        Returns:
          A list of the tasks that were woken up.
        """
        return self.unpark(count=len(self))

    @_core.enable_ki_protection
    def unpark_matching(self, condition, *, count=1):
        """Unpark one or more tasks that are sleeping in a call
        to :meth:`park` whose cookie satisfies some condition.

        This wakes up the ``count`` tasks that have been blocked for
        the longest in calls to ``park(cookie)`` whose ``cookie``
        would make ``condition(cookie)`` return true. If there are
        fewer than ``count`` such tasks, all of them are woken.

        :meth:`unpark_matching` will continue to wake up tasks that
        satisfy the ``condition`` even if other tasks that do not
        satisfy the ``condition`` have been waiting for longer. If you
        want to process wakeups in strict FIFO order, use
        :meth:`unpark_if` or :meth:`unpark_while` instead.

        Args:
          condition: A function of one argument that takes a cookie
              that was passed to :meth:`park` and returns true if the
              task that's blocked in that call should be woken up.
              Calls will be made in the order in which tasks would be
              woken up. The function must not make reentrant calls to
              methods of this :class:`ParkingLot`.
          count (int): The maximum number of tasks to wake up.

        Returns:
          A list of the tasks that were woken up.

        """
        if count <= 0:
            return []
        tasks = []
        for task, cookie in self._parked.items():
            if condition(cookie):
                tasks.append(task)
                if len(tasks) >= count:
                    break

        # Only wake tasks if none of the condition() calls threw an
        # exception
        for task in tasks:
            del self._parked[task]
            _core.reschedule(task)
        return tasks

    @_core.enable_ki_protection
    def unpark_if(self, condition, *, count=1):
        """Unpark one or more tasks that are next in line to be woken,
        and are sleeping in a call to :meth:`park` whose cookie
        satisfies some condition.

        This behaves like :meth:`unpark`, except that it stops waking
        up tasks when it reaches one that's blocked in a call to
        ``park(cooke)`` whose ``cookie`` would not make ``condition(cookie)``
        return true. Unlike :meth:`unpark_matching`, it will never
        skip over a task that does not satisfy the condition in order
        to wake one that does. This improves fairness, but might
        reduce throughput; the correct choice between :meth:`unpark_if`
        and :meth:`unpark_matching` depends on your application.

        Args:
          condition: A function of one argument that takes a cookie
              that was passed to :meth:`park` and returns true if the
              task that's blocked in that call should be woken up.
              Calls will be made in the order in which tasks would be
              woken up. The function must not make reentrant calls to
              methods of this :class:`ParkingLot`.
          count (int): The maximum number of tasks to wake up.

        Returns:
          A list of the tasks that were woken up.

        """
        if count <= 0:
            return []
        tasks = []
        for task, cookie in self._parked.items():
            if condition(cookie):
                tasks.append(task)
                if len(tasks) >= count:
                    break
            else:
                break  # <-- this is the only difference from unpark_matching()

        # Only wake tasks if none of the condition() calls threw an
        # exception
        for task in tasks:
            del self._parked[task]
            _core.reschedule(task)
        return tasks

    def unpark_while(self, condition):
        """Unpark all of the tasks that are next in line to be woken and
        are sleeping in a call to :meth:`park` whose cookie satisfies
        some condition.

        Equivalent to ``unpark_if(condition, count=math.inf)``.

        Args:
          condition: A function of one argument that takes a cookie
              that was passed to :meth:`park` and returns true if the
              task that's blocked in that call should be woken up.
              Calls will be made in the order in which tasks would be
              woken up.

        Returns:
          A list of the tasks that were woken up.

        """
        return self.unpark_if(condition, count=len(self))

    @_core.enable_ki_protection
    def repark(self, new_lot, *, count=1):
        """Move parked tasks from one :class:`ParkingLot` object to another.

        This dequeues ``count`` tasks from one lot, and requeues them on
        another, preserving order. For example::

           async def parker(lot):

               print("sleeping")
               await lot.park()
               print("woken")

           async def main():
               lot1 = trio.hazmat.ParkingLot()
               lot2 = trio.hazmat.ParkingLot()
               async with trio.open_nursery() as nursery:
                   nursery.start_soon(parker, lot1)
                   await trio.testing.wait_all_tasks_blocked()
                   assert len(lot1) == 1
                   assert len(lot2) == 0
                   lot1.repark(lot2)
                   assert len(lot1) == 0
                   assert len(lot2) == 1
                   # This wakes up the task that was originally parked in lot1
                   lot2.unpark()

        If there are fewer than ``count`` tasks parked, then reparks as many
        tasks as are available and then returns successfully.

        Args:
          new_lot (ParkingLot): the parking lot to move tasks to.
          count (int): the number of tasks to move.

        """
        if not isinstance(new_lot, ParkingLot):
            raise TypeError("new_lot must be a ParkingLot")
        for task, cookie in self._pop_several(count):
            new_lot._parked[task] = cookie
            task.custom_sleep_data = new_lot

    def repark_all(self, new_lot):
        """Move all parked tasks from one :class:`ParkingLot` object to
        another.

        See :meth:`repark` for details.

        """
        return self.repark(new_lot, count=len(self))

    def statistics(self):
        """Return an object containing debugging information.

        Currently the following fields are defined:

        * ``tasks_waiting``: The number of tasks blocked on this lot's
          :meth:`park` method.

        """
        return _ParkingLotStatistics(tasks_waiting=len(self._parked))
