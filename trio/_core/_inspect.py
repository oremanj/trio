import ctypes, dis, gc, inspect, sys
from types import FrameType
from typing import List
from . import _run


def _contexts_visible_in_frame(frame: FrameType) -> List[object]:
    """Inspects the frame object ``frame`` to try to determine which context
    managers are currently active; returns a list from outermost to innermost.
    The frame must be suspended at a yield or await.
    """

    assert sys.implementation.name == "cpython"

    # This is the layout of the start of a frame object. It has a couple
    # fields we can't access from Python, especially f_valuestack and
    # f_stacktop.
    class FrameObjectStart(ctypes.Structure):
        _fields_ = [
            ("ob_refcnt", ctypes.c_ulong), # reference count
            ("ob_type", ctypes.c_ulong), # PyTypeObject*
            ("ob_size", ctypes.c_ulong), # number of pointers after f_localsplus
            ("f_back", ctypes.c_ulong), # PyFrameObject*
            ("f_code", ctypes.c_ulong), # PyCodeObject*
            ("f_builtins", ctypes.c_ulong), # PyDictObject*
            ("f_globals", ctypes.c_ulong), # PyDictObject*
            ("f_locals", ctypes.c_ulong), # PyObject*, some mapping
            ("f_valuestack", ctypes.c_ulong), # PyObject**, points within self
            ("f_stacktop", ctypes.c_ulong), # PyObject**, points within self
            # and then we start seeing differences between different
            # Python versions
        ]

    # Basic sanity checks cross-referencing the values we can get from Python
    # with their Python values
    frame_raw = FrameObjectStart.from_address(id(frame))
    refcnt = frame_raw.ob_refcnt
    assert refcnt + 1 == sys.getrefcount(frame)
    assert frame_raw.ob_type == id(type(frame))
    assert frame_raw.f_back == (
        id(frame.f_back) if frame.f_back is not None else 0
    )
    assert frame_raw.f_code == id(frame.f_code)
    assert frame_raw.f_globals == id(frame.f_globals)

    # The frame object has a fixed-length part followed by f_localsplus
    # which is a variable-length array of PyObject*. The array contains
    # co_nlocals + len(co_cellvars) + len(co_freevars) slots for
    # those things, followed by co_stacksize slots for the bytecode
    # interpreter stack. f_valuestack points at the beginning of the
    # stack part. Figure out where f_localsplus is. (It's a constant
    # offset from the start of the frame, but the constant differs
    # by Python version.)
    co = frame.f_code
    wordsize = ctypes.sizeof(ctypes.c_ulong)
    stack_start_offset = frame_raw.f_valuestack - id(frame)
    localsplus_offset = stack_start_offset - wordsize * (
        co.co_nlocals + len(co.co_cellvars) + len(co.co_freevars)
    )
    end_offset = stack_start_offset + wordsize * co.co_stacksize

    # Make sure our inferred size for the overall frame object matches
    # what Python says and what the ob_size field says. Note ob_size can
    # be larger than necessary due to frame object reuse.
    assert frame_raw.ob_size >= (end_offset - localsplus_offset) / wordsize
    assert end_offset == frame.__sizeof__()

    # Figure out what portion of the stack is actually valid, and extract
    # the PyObject pointers. We just store their addresses (id), not taking
    # references or anything.
    stack_top_offset = frame_raw.f_stacktop - id(frame)
    assert stack_start_offset <= stack_top_offset <= end_offset
    stack = [
        ctypes.c_ulong.from_address(id(frame) + offset).value
        for offset in range(stack_start_offset, stack_top_offset, wordsize)
    ]
    # Now stack[i] corresponds to f_stacktop[i] in C.

    # Figure out the active context managers. Each context manager
    # pushes a block to a fixed-size block stack (20 12-byte entries,
    # this has been unchanged for ages) which is stored by value right
    # before f_localsplus. There's another frame field for the size of
    # the block stack.
    class PyTryBlock(ctypes.Structure):
        _fields_ = [
            # An opcode; the blocks we want are SETUP_FINALLY
            ("b_type", ctypes.c_int),

            # An offset in co.co_code; the blocks we want have a
            # WITH_CLEANUP_START opcode at this offset
            ("b_handler", ctypes.c_int),

            # An index on the value stack; if we're still in the body
            # of the with statement, the blocks we want have
            # an __exit__ or __aexit__ method at stack index b_level - 1
            ("b_level", ctypes.c_int),
        ]

    active_context_exit_ids = []
    coro_aexiting_now_id = None

    blockstack_offset = localsplus_offset - 20 * ctypes.sizeof(PyTryBlock)
    f_iblock = ctypes.c_int.from_address(id(frame) + blockstack_offset - 8)
    f_lasti = ctypes.c_int.from_address(id(frame) + blockstack_offset - 16)
    assert f_lasti.value == frame.f_lasti
    assert 0 <= f_iblock.value <= 20
    assert blockstack_offset > ctypes.sizeof(FrameObjectStart)

    blockstack_end_offset = blockstack_offset + (
        f_iblock.value * ctypes.sizeof(PyTryBlock)
    )
    assert blockstack_offset <= blockstack_end_offset <= localsplus_offset

    # Process blocks on the current block stack
    while blockstack_offset < blockstack_end_offset:
        block = PyTryBlock.from_address(id(frame) + blockstack_offset)
        assert (
            0 < block.b_type <= 257
            and (
                # EXCEPT_HANDLER blocks (type 257) have a bogus b_handler
                block.b_handler == -1 if block.b_type == 257
                else 0 < block.b_handler < len(co.co_code)
            )
            and 0 <= block.b_level <= len(stack)
        )

        # Looks like a valid block -- is it one of our context managers?
        if (
            block.b_type == dis.opmap["SETUP_FINALLY"] and
            co.co_code[block.b_handler] == dis.opmap["WITH_CLEANUP_START"]
        ):
            # Yup. Still fully inside the block; use b_level to find the
            # __exit__ or __aexit__ method
            active_context_exit_ids.append(stack[block.b_level - 1])

        blockstack_offset += ctypes.sizeof(PyTryBlock)

    # Decide whether it looks like we're in the middle of an
    # __aexit__, which would already have been popped from the block
    # stack.  In this case we can only get the coroutine, not the
    # original __aexit__ method.
    if list(co.co_code[frame.f_lasti-4 : frame.f_lasti+4 : 2]) == [
        dis.opmap["WITH_CLEANUP_START"],
        dis.opmap["GET_AWAITABLE"],
        dis.opmap["LOAD_CONST"],
        dis.opmap["YIELD_FROM"]
    ]:
        coro_aexiting_now_id = stack[-1]

    # Now active_context_exit_ids contains (in theory) the addresses
    # of the __exit__/__aexit__ callables for each context manager
    # that's activated in this frame, from outermost to innermost. If
    # an async context manager is aexiting now, we additionally set
    # coro_aexiting_now_id to its id.  We'll add another layer of
    # security by getting the actual objects out of gc.get_referents()
    # rather than just casting the addresses.

    object_from_id_map = {id(obj): obj for obj in gc.get_referents(frame)
                          if id(obj) in active_context_exit_ids
                          or id(obj) == coro_aexiting_now_id}
    assert len(object_from_id_map) == len(active_context_exit_ids) + (
        coro_aexiting_now_id is not None
    )

    active_contexts = []
    for ident in active_context_exit_ids:
        active_contexts.append(object_from_id_map[ident].__self__)

    if coro_aexiting_now_id is not None:
        # Assume the context manager is the 1st coroutine argument.
        coro = object_from_id_map[coro_aexiting_now_id]
        args = inspect.getargvalues(coro.cr_frame)
        active_contexts.append(args.locals[args.args[0]])
    return active_contexts


def _frame_and_next(awaitable):
    typename = type(awaitable).__name__

    if typename == "coroutine":
        return awaitable.cr_frame, awaitable.cr_await
    if typename == "generator":
        return awaitable.gi_frame, awaitable.gi_yieldfrom
    if typename == "ANextIter":  # @async_generator awaitable
        return _frame_and_next(awaitable._it)
    if typename in ("async_generator_asend", "async_generator_athrow"):
        # native async generator awaitable, which holds a
        # reference to its agen but doesn't expose it
        for referent in gc.get_referents(awaitable):
            if hasattr(referent, "ag_frame"):
                return referent.ag_frame, referent.ag_await
    if typename == "coroutine_wrapper":
        # these refer to only one other object, the underlying coroutine
        for referent in gc.get_referents(awaitable):
            return _frame_and_next(referent)
    raise RuntimeError(f"Not sure what to do with a {awaitable!r}")
    #return None, None


def _frames_suspended_in_task(task):
    if task.coro.cr_running:
        # the technique we're using doesn't work for frames that are not
        # suspended
        return []

    gen = task.coro
    frames = []
    while gen is not None:
        frame, gen = _frame_and_next(gen)
        if frame is None or frame.f_lasti == -1:
            # closed or just-created coroutine can't have any
            # contexts active
            break
        frames.append(frame)
    return frames


def _map_trio_scopes_to_toplevel_contexts(trio_scopes, toplevel_contexts):
    refers_to_trio_scope = collections.OrderedDict()
    referenced_by_toplevel_context = collections.OrderedDict()
    # ...


def _dump_traceback_with_trio_contexts(task):
    frames = _frames_suspended_in_task(task)
    for frame in frames:
        filename, lineno, function, _, _ = inspect.getframeinfo(frame, context=0)
        for context in _contexts_visible_in_frame(frame):
            # ...
            pass


class GuessContextsInstrument:
    def task_scheduled(self, task):
      try:
        frames = _frames_suspended_in_task(task)
        for frame in frames:
            filename, lineno, function, _, _ = inspect.getframeinfo(frame, context=0)
            for context in _contexts_visible_in_frame(frame):
                pass #print(f"    {context!r}")
            #print(f"{function} at {filename}:{lineno}")
        if frames:
            pass #print("---")
      except:
        import traceback; traceback.print_exc()
        import pdb; pdb.pm()
