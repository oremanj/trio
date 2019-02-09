import pytest

from ... import _core
from ...testing import wait_all_tasks_blocked
from .tutil import check_sequence_matches
from .._parking_lot import ParkingLot


async def test_parking_lot_basic():
    record = []

    async def waiter(i, lot):
        record.append("sleep {}".format(i))
        await lot.park()
        record.append("wake {}".format(i))

    async with _core.open_nursery() as nursery:
        lot = ParkingLot()
        assert not lot
        assert len(lot) == 0
        assert lot.statistics().tasks_waiting == 0
        for i in range(3):
            nursery.start_soon(waiter, i, lot)
        await wait_all_tasks_blocked()
        assert len(record) == 3
        assert bool(lot)
        assert len(lot) == 3
        assert lot.statistics().tasks_waiting == 3
        lot.unpark_all()
        assert lot.statistics().tasks_waiting == 0
        await wait_all_tasks_blocked()
        assert len(record) == 6

    check_sequence_matches(
        record, [
            {"sleep 0", "sleep 1", "sleep 2"},
            {"wake 0", "wake 1", "wake 2"},
        ]
    )

    async with _core.open_nursery() as nursery:
        record = []
        for i in range(3):
            nursery.start_soon(waiter, i, lot)
            await wait_all_tasks_blocked()
        assert len(record) == 3
        for i in range(3):
            lot.unpark()
            await wait_all_tasks_blocked()
        # 1-by-1 wakeups are strict FIFO
        assert record == [
            "sleep 0",
            "sleep 1",
            "sleep 2",
            "wake 0",
            "wake 1",
            "wake 2",
        ]

    # It's legal (but a no-op) to try and unpark while there's nothing parked
    lot.unpark()
    lot.unpark(count=1)
    lot.unpark(count=100)

    # Check unpark with count
    async with _core.open_nursery() as nursery:
        record = []
        for i in range(3):
            nursery.start_soon(waiter, i, lot)
            await wait_all_tasks_blocked()
        lot.unpark(count=2)
        await wait_all_tasks_blocked()
        check_sequence_matches(
            record, [
                "sleep 0",
                "sleep 1",
                "sleep 2",
                {"wake 0", "wake 1"},
            ]
        )
        lot.unpark_all()


async def cancellable_waiter(name, lot, scopes, record):
    with _core.CancelScope() as scope:
        scopes[name] = scope
        record.append("sleep {}".format(name))
        try:
            await lot.park(name)
        except _core.Cancelled:
            record.append("cancelled {}".format(name))
        else:
            record.append("wake {}".format(name))


async def test_parking_lot_cancel():
    record = []
    scopes = {}

    async with _core.open_nursery() as nursery:
        lot = ParkingLot()
        nursery.start_soon(cancellable_waiter, 1, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 2, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 3, lot, scopes, record)
        await wait_all_tasks_blocked()
        assert len(record) == 3

        scopes[2].cancel()
        await wait_all_tasks_blocked()
        assert len(record) == 4
        lot.unpark_all()
        await wait_all_tasks_blocked()
        assert len(record) == 6

    check_sequence_matches(
        record, [
            "sleep 1",
            "sleep 2",
            "sleep 3",
            "cancelled 2",
            {"wake 1", "wake 3"},
        ]
    )


async def test_parking_lot_conditional_wakeups():
    record = []
    scopes = {}

    async with _core.open_nursery() as nursery:
        lot = ParkingLot()
        nursery.start_soon(cancellable_waiter, 1, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 2, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 3, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 4, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 5, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 6, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 7, lot, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 8, lot, scopes, record)
        await wait_all_tasks_blocked()
        assert len(record) == 8

        for count in (-1, 0):
            assert not lot.unpark_if(bool, count=count)
            assert not lot.unpark_matching(bool, count=count)

        def is_even(n):
            return n % 2 == 0

        # next in line (1) isn't even, so no wakeups
        assert not lot.unpark_if(is_even)
        await wait_all_tasks_blocked()
        assert len(record) == 8

        # wake up 2 and 4, limited by count
        # now lot is [1, 3, 5, 6, 7, 8]
        assert 2 == len(lot.unpark_matching(is_even, count=2))
        await wait_all_tasks_blocked()
        assert len(record) == 10

        # wake up 1, limited by count
        # now lot is [3, 5, 6, 7, 8]
        assert 1 == len(lot.unpark_if(lambda n: n <= 3))
        await wait_all_tasks_blocked()
        assert len(record) == 11

        assert 2 == lot.count_matching(is_even)

        # wake up 3 and 5, limited by condition
        # now lot is [6, 7, 8]
        assert 2 == len(lot.unpark_while(lambda n: not is_even(n)))
        await wait_all_tasks_blocked()
        assert len(record) == 13

        # cancel 7; now lot is [6, 8]
        scopes[7].cancel()
        await wait_all_tasks_blocked()
        assert len(record) == 14

        # wake up 6 and 8
        assert 2 == len(lot.unpark_while(is_even))
        await wait_all_tasks_blocked()
        assert len(record) == 16

        assert not lot.unpark_if(is_even)
        assert not lot.unpark_matching(is_even)

    check_sequence_matches(
        record, [
            "sleep 1",
            "sleep 2",
            "sleep 3",
            "sleep 4",
            "sleep 5",
            "sleep 6",
            "sleep 7",
            "sleep 8",
            {"wake 2", "wake 4"},
            "wake 1",
            {"wake 3", "wake 5"},
            "cancelled 7",
            {"wake 6", "wake 8"},
        ]
    )


async def test_parking_lot_repark():
    record = []
    scopes = {}
    lot1 = ParkingLot()
    lot2 = ParkingLot()

    with pytest.raises(TypeError):
        lot1.repark([])

    async with _core.open_nursery() as nursery:
        nursery.start_soon(cancellable_waiter, 1, lot1, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 2, lot1, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 3, lot1, scopes, record)
        await wait_all_tasks_blocked()
        assert len(record) == 3

        assert len(lot1) == 3
        lot1.repark(lot2)
        assert len(lot1) == 2
        assert len(lot2) == 1
        lot2.unpark_all()
        await wait_all_tasks_blocked()
        assert len(record) == 4
        assert record == ["sleep 1", "sleep 2", "sleep 3", "wake 1"]

        lot1.repark_all(lot2)
        assert len(lot1) == 0
        assert len(lot2) == 2

        scopes[2].cancel()
        await wait_all_tasks_blocked()
        assert len(lot2) == 1
        assert record == [
            "sleep 1", "sleep 2", "sleep 3", "wake 1", "cancelled 2"
        ]

        # make sure cookies are preserved during reparking
        assert 1 == len(lot2.unpark_if(lambda n: n == 3))
        await wait_all_tasks_blocked()
        assert record == [
            "sleep 1", "sleep 2", "sleep 3", "wake 1", "cancelled 2", "wake 3"
        ]


async def test_parking_lot_repark_with_count():
    record = []
    scopes = {}
    lot1 = ParkingLot()
    lot2 = ParkingLot()
    async with _core.open_nursery() as nursery:
        nursery.start_soon(cancellable_waiter, 1, lot1, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 2, lot1, scopes, record)
        await wait_all_tasks_blocked()
        nursery.start_soon(cancellable_waiter, 3, lot1, scopes, record)
        await wait_all_tasks_blocked()
        assert len(record) == 3

        assert len(lot1) == 3
        assert len(lot2) == 0
        lot1.repark(lot2, count=2)
        assert len(lot1) == 1
        assert len(lot2) == 2
        while lot2:
            lot2.unpark()
            await wait_all_tasks_blocked()
        assert record == [
            "sleep 1",
            "sleep 2",
            "sleep 3",
            "wake 1",
            "wake 2",
        ]
        lot1.unpark_all()
