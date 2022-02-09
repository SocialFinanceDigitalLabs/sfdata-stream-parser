from sfdata_stream_parser import events


def test_event_asdict():
    cell = events.Cell(value="Test Value", column_index=4, context=dict(test="value"))
    assert cell.as_dict() == {
        "value": "Test Value",
        "column_index": 4,
        "context": {"test": "value"}
    }


def test_event_clone():
    cell = events.Cell(value="Test Value", column_index=4, context=dict(test="value"))
    cloned_cell = events.Cell.from_event(cell, value="A new value")

    assert cloned_cell.value == "A new value"
    assert cloned_cell.source == cell
    assert cloned_cell.source.value == "Test Value"
