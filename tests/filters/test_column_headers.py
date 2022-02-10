import pytest

from sfdata_stream_parser.parser import parse_csv
from sfdata_stream_parser.filters.column_headers import promote_first_row, _SafeList, header_wrapper
from sfdata_stream_parser import events


@pytest.fixture
def single_table():
    return parse_csv([
        "Col1,Col2,Col3",
        "R1C1,R1C2,R1C3",
        "R2C1,R2C2,R2C3",
        "R3C1,R3C2,R3C3",
    ], name="test_csv", table_name="test_table")


def test_safe_list():
    my_list = _SafeList([1, 2, 3, 4])
    assert my_list.get(0) == 1
    assert my_list.get(1) == 2
    assert my_list.get(2) == 3
    assert my_list.get(3) == 4
    assert my_list.get(4) is None
    assert my_list.get(-5) is None
    assert my_list.get(99) is None
    assert my_list.get(99, "DEFAULT") == "DEFAULT"


def test_column_headers_start_rows(single_table):
    event_list = list(promote_first_row(single_table))
    row_list = list(filter(lambda x: isinstance(x, events.StartRow), event_list))
    assert len(row_list) == 3
    assert [x.row_index for x in row_list] == [0, 1, 2]
    assert [x.headers for x in row_list] == [['Col1', 'Col2', 'Col3']] * 3


def test_column_headers_extra_events():
    class DummyEvent(events.ParseEvent):
        pass

    stream = iter((
        events.StartTable(ix=0),
        DummyEvent(ix=1),
        DummyEvent(ix=2),
        events.StartRow(ix=3),
        events.Cell(ix=4),
        events.EndRow(ix=5),
        events.StartRow(ix=6),
        events.Cell(ix=7),
        events.EndRow(ix=8),
        events.EndTable(ix=9),
    ))
    event_list = list(header_wrapper(stream))
    assert len(event_list) == 7

    assert isinstance(event_list[1], DummyEvent)
    assert isinstance(event_list[2], DummyEvent)

    assert [e.ix for e in event_list] == [0, 1, 2, 6, 7, 8, 9]


def test_header_wrapper_non_iterator():
    with pytest.raises(AssertionError) as excinfo:
        list(header_wrapper((events.StartTable(),)))

    assert 'Source must be an iterator' in str(excinfo.value)


def test_header_wrapper_wrong_start():
    stream = iter((events.StartRow(),))
    with pytest.raises(AssertionError) as excinfo:
        list(header_wrapper(stream))

    assert 'Expected StartTable event' in str(excinfo.value)

