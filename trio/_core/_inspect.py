import collections, ctypes, dis, gc, inspect, math, sys
from types import FrameType
from typing import List, Tuple, Optional, Iterator
from . import _run, _ki


def _contexts_active_in_frame(frame: FrameType) -> List[Tuple[object, Optional[str], int]]:
    """Inspects the frame object ``frame`` to try to determine which context
    managers are currently active; returns a list of tuples
    (manager, name, start_line) describing the active context managers
    from outermost to innermost. ``manager`` is the context manager object.
    ``name`` is the name of the local variable that the result of its
    ``__enter__`` or ``__aenter__`` method was assigned to. ``start_line``
    is the line number on which the ``with`` or ``async with`` block began.

    The frame must be suspended at a yield or await. Raises AssertionError
    if we couldn't make sense of this frame.
    """

    assert sys.implementation.name == "cpython"

    # Bytecode analysis to figure out where each 'with' or 'async with'
    # block starts, so we can look up the extra info based on the bytecode
    # offset of the WITH_CLEANUP_START instruction later on.
    with_block_info: Dict[int, Tuple[Optional[str], int]] = {}
    insns = list(dis.Bytecode(frame.f_code))
    current_line = frame.f_code.co_firstlineno
    for insn, inext in zip(insns[:-1], insns[1:]):
        if insn.starts_line is not None:
            current_line = insn.starts_line
        if insn.opname in ("SETUP_WITH", "SETUP_ASYNC_WITH"):
            if inext.opname == "STORE_FAST":
                store_to = inext.argval
            else:
                store_to = None
            cleanup_offset = insn.argval
            with_block_info[cleanup_offset] = (store_to, current_line)

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
    active_context_exit_offsets = []
    coro_aexiting_now_id = None
    coro_aexiting_now_offset = None

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
            active_context_exit_offsets.append(block.b_handler)

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
        coro_aexiting_now_offset = frame.f_lasti - 4

    # Now active_context_exit_ids contains (in theory) the addresses
    # of the __exit__/__aexit__ callables for each context manager
    # that's activated in this frame, from outermost to innermost. If
    # an async context manager is aexiting now, we additionally set
    # coro_aexiting_now_id to its id.  We'll add another layer of
    # security by getting the actual objects out of gc.get_referents()
    # rather than just casting the addresses.

    object_from_id_map = {
        id(obj): obj
        for obj in gc.get_referents(frame)
        if id(obj) in active_context_exit_ids
        or id(obj) == coro_aexiting_now_id
    }
    assert len(object_from_id_map) == len(active_context_exit_ids) + (
        coro_aexiting_now_id is not None
    )

    active_context_info = []
    for ident, offset in zip(active_context_exit_ids, active_context_exit_offsets):
        active_context_info.append(
            (object_from_id_map[ident].__self__, *with_block_info[offset])
        )

    if coro_aexiting_now_id is not None:
        # Assume the context manager is the 1st coroutine argument.
        coro = object_from_id_map[coro_aexiting_now_id]
        args = inspect.getargvalues(coro.cr_frame)
        active_context_info.append(
            (args.locals[args.args[0]], *with_block_info[coro_aexiting_now_offset])
        )

    return active_context_info


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
    return None, None


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


def _match_trio_scopes_to_contexts(trio_scopes, toplevel_contexts):
    trio_scope_to_context = {}
    unmatched_trio_scopes = set(id(scope) for scope in trio_scopes)
    to_visit = collections.deque(
        (ctx, ctx, 0) for ctx in toplevel_contexts
    )
    while unmatched_trio_scopes and to_visit:
        obj, root, depth = to_visit.popleft()
        if id(obj) in unmatched_trio_scopes:
            trio_scope_to_context[id(obj)] = root
            unmatched_trio_scopes.remove(id(obj))
        if depth < 5:
            to_visit.extend(
                (referent, root, depth + 1)
                for referent in gc.get_referents(obj)
            )
    return trio_scope_to_context


def format_task_tree(
    root: Optional[_run.Task] = None, prefix: str = ""
) -> Iterator[str]:

    if root is None:
        root = _run.current_root_task()
        if root is None:
            return
    yield f"{prefix}task '{root.name}':"
    if prefix.endswith("-- "):
        prefix = prefix[:-3] + "   "

    frames = _frames_suspended_in_task(root)
    contexts_per_frame = [_contexts_active_in_frame(frame) for frame in frames]
    all_contexts = [ctx for sublist in contexts_per_frame for ctx, *info in sublist]
    all_trio_scopes = collections.deque()

    if root.parent_nursery is not None:
        inherited_from_parent = len(root.parent_nursery._cancel_stack)
    else:
        inherited_from_parent = 0
    cancel_scopes = root._cancel_stack[inherited_from_parent:][::-1]
    for nursery in root.child_nurseries:
        while cancel_scopes[-1] is not nursery.cancel_scope:
            all_trio_scopes.append(cancel_scopes.pop())
        # don't include the nursery's cancel scope since we print it
        # as part of the nursery
        cancel_scopes.pop()
        all_trio_scopes.append(nursery)
    all_trio_scopes.extend(cancel_scopes[::-1])

    scope_map = _match_trio_scopes_to_contexts(all_trio_scopes, all_contexts)

    def format_trio_scope(scope, filename, info) -> Iterator[str]:
        nonlocal prefix
        if info is not None:
            varname, lineno = info
        else:
            varname, lineno, filename = None, "??", "??"
        nameinfo = f"`{varname}' " if varname is not None else ""
        where = f"{nameinfo}at {filename}:{lineno}"

        def cancel_scope_info(cancel_scope):
            bits = []
            if cancel_scope.cancel_called:
                bits.append("cancelled")
            elif cancel_scope.deadline != math.inf:
                bits.append(
                    "timeout in {:.2f}sec".format(
                        cancel_scope.deadline - _run.current_time()
                    )
                )
            if cancel_scope.shield:
                bits.append("shielded")
            return (": " if bits else "") + ", ".join(bits)

        if type(scope).__name__ == "CancelScope":
            yield f"{prefix}cancel scope {where}" + cancel_scope_info(scope)

        elif type(scope).__name__ == "Nursery":
            yield f"{prefix}nursery {where}" + cancel_scope_info(scope.cancel_scope)
            for task in scope.child_tasks:
                yield from format_task_tree(task, prefix + "|-- ")
            yield f"{prefix}+-- nested child:"
            prefix += "    "

        else:
            yield f"{prefix}<!> unhandled {scope!r} {where}"

    for frame, contexts_info in zip(frames, contexts_per_frame):
        filename, lineno, function, _, _ = inspect.getframeinfo(frame, context=0)
        context_to_info = {id(context): info for context, *info in contexts_info}
        while all_trio_scopes:
            context = scope_map.get(id(all_trio_scopes[0]))
            if context is not None and id(context) not in context_to_info:
                # This context belongs to the next frame -- don't print it
                # in this frame
                break
            yield from format_trio_scope(
                all_trio_scopes.popleft(), filename, context_to_info.get(id(context))
            )
        #argvalues = inspect.formatargvalues(*inspect.getargvalues(frame))
        argvalues = inspect.getargvalues(frame)
        if argvalues.args and argvalues.args[0] == "self":
            function = f"{type(argvalues.locals['self']).__name__}.{function}"
        yield f"{prefix}{function} at {filename}:{lineno}"
    yield prefix
