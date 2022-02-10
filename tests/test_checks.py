from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check


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


