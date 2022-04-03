from typing import Iterable
from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.collectors import collector, block_check
from sfdata_stream_parser.filters.generic import filter_stream, streamfilter
from sfdata_stream_parser.functions import pass_event


class _SafeList(list):
    def get(self, index, default=None):
        try:
            return self.__getitem__(index)
        except IndexError:
            return default


class _HeaderEvent(events.ParseEvent):
    def __init__(self, headers: Iterable[str]):
        super().__init__(headers=headers)


@collector(check=block_check(start_type=events.StartTable), receive_stream=True)
def promote_first_row(stream):
    """
    Promote the first row of tables as column headers. The headers are set on the StartTable event as well
    as on the individual cells.
    """
    start_table = next(stream)
    assert isinstance(start_table, events.StartTable), "Expected StartTable event"

    @collector(check=block_check(start_type=events.StartRow), receive_stream=True, stop_after=True, iterations=1)
    def row_consumer(row):
        row = list(row)
        _headers = []
        for event in filter_stream(row, type_check(events.Cell)):
            _headers.append(event.get("value", ""))
        yield _HeaderEvent(_headers)

    header_events = list(row_consumer(stream))
    headers = header_events[-1].headers

    yield events.StartTable.from_event(start_table, column_headers=headers)
    yield from header_events[:-1]

    @streamfilter(check=type_check(events.StartRow), fail_function=pass_event)
    def row_enricher(event):
        yield event.from_event(event, headers=headers)

    stream = row_enricher(stream)
    yield from stream
