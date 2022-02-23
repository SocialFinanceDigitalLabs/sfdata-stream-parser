import pytest

from sfdata_stream_parser.parser.csv import parse_csv
from sfdata_stream_parser import events


@pytest.fixture
def event_stream():
    return parse_csv([
        "Col1,Col2,Col3",
        "R1C1,R1C2,R1C3",
        "R2C1,R2C2,R2C3",
        "R3C1,R3C2,R3C3",
    ], name="test_csv", table_name="test_table")


def test_csv_emitter_types(event_stream):
    stream = list(event_stream)
    assert [type(e) for e in stream[:7]] == [events.StartContainer, events.StartTable, events.StartRow,
                                             events.Cell, events.Cell, events.Cell, events.EndRow]

    assert [type(e) for e in stream[-7:]] == [events.StartRow, events.Cell, events.Cell, events.Cell,
                                              events.EndRow, events.EndTable, events.EndContainer]


def test_csv_emitter_values(event_stream):
    stream = list(event_stream)
    assert [e.value for e in stream if hasattr(e, 'value')] == [
        "Col1", "Col2", "Col3",
        "R1C1", "R1C2", "R1C3",
        "R2C1", "R2C2", "R2C3",
        "R3C1", "R3C2", "R3C3",
    ]


def test_csv_emitter_names(event_stream):
    stream = list(event_stream)
    assert [e.name for e in stream if hasattr(e, 'name')] == [
        'test_csv', 'test_table', 'test_table', 'test_csv',
    ]


def test_csv_emitter_row_and_cols(event_stream):
    stream = list(event_stream)

    def get_rc(e):
        try:
            return f'r{e.row_index}'
        except AttributeError:
            pass
        try:
            return f'c{e.column_index}'
        except AttributeError:
            pass
        return None

    assert [get_rc(e) for e in stream] == [
        None, None,
        'r0', 'c0', 'c1', 'c2', 'r0',
        'r1', 'c0', 'c1', 'c2', 'r1',
        'r2', 'c0', 'c1', 'c2', 'r2',
        'r3', 'c0', 'c1', 'c2', 'r3',
        None, None
    ]



