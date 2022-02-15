from unittest.mock import MagicMock

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check, and_check, or_check


def test_type_check_single():
    check = type_check(events.StartRow)
    assert check(events.StartRow())
    assert not check(events.EndRow())


def test_type_check_multiple():
    check = type_check((events.StartRow, events.EndRow))
    assert check(events.StartRow())
    assert check(events.EndRow())
    assert not check(events.Cell())


def test_type_check_empty():
    check = type_check()
    assert not check(events.StartRow())
    assert not check(events.Cell())


def test_type_check_empty_permissive():
    check = type_check(permissive=True)
    assert check(events.StartRow())
    assert check(events.Cell())


def test_and_check():
    ev = events.StartRow()

    c1 = MagicMock(return_value=True)
    c2 = MagicMock(return_value=False)

    check = and_check(c1, c2)
    assert not check(ev)

    c1.assert_called_once_with(ev)
    c2.assert_called_once_with(ev)

    c1 = MagicMock(return_value=True)
    c2 = MagicMock(return_value=True)

    check = and_check(c1, c2)
    assert check(ev)

    c1.assert_called_once_with(ev)
    c2.assert_called_once_with(ev)

    c1 = MagicMock(return_value=False)
    c2 = MagicMock(return_value=True)

    check = and_check(c1, c2)
    assert not check(ev)

    c1.assert_called_once_with(ev)
    c2.assert_not_called()

    c1 = MagicMock(return_value=True)
    c2 = MagicMock(return_value=False)

    check = and_check(c1, c1, c1, c1, c1, c1, c2, c1, c1)
    assert not check(ev)

    c1.assert_called_with(ev)
    assert c1.call_count == 6
    c2.assert_called_once_with(ev)


def test_or_check():
    ev = events.StartRow()

    c1 = MagicMock(return_value=True)
    c2 = MagicMock(return_value=False)

    check = or_check(c1, c2)
    assert check(ev)

    c1.assert_called_once_with(ev)
    c2.assert_not_called()

    c1 = MagicMock(return_value=False)
    c2 = MagicMock(return_value=True)

    check = or_check(c1, c2)
    assert check(ev)

    c1.assert_called_once_with(ev)
    c2.assert_called_once_with(ev)

    c1 = MagicMock(return_value=False)
    c2 = MagicMock(return_value=False)

    check = or_check(c1, c2)
    assert not check(ev)

    c1.assert_called_once_with(ev)
    c2.assert_called_once_with(ev)

    c1 = MagicMock(return_value=False)
    c2 = MagicMock(return_value=True)

    check = or_check(c1, c1, c1, c1, c1, c1, c2, c1, c1)
    assert check(ev)

    c1.assert_called_with(ev)
    assert c1.call_count == 6
    c2.assert_called_once_with(ev)