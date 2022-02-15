from typing import Iterable, Iterator

from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import conditional_wrapper


class _SafeList(list):
    def get(self, index, default=None):
        try:
            return self.__getitem__(index)
        except IndexError:
            return default


def promote_first_row(source: Iterable[events.ParseEvent]) -> Iterable[events.ParseEvent]:
    """
    Promote the first row of tables as column headers. The headers are set on the StartTable event as well
    as on the individual cells.
    """
    yield from conditional_wrapper(
        source,
        header_wrapper,
        lambda e: isinstance(e, events.StartTable),
    )


def header_wrapper(source: Iterator[events.ParseEvent]) -> Iterator[events.ParseEvent]:
    assert hasattr(source, "__next__"), "Source must be an iterator"
    peekable_source = peekable(source)

    start_table = next(peekable_source)
    assert isinstance(start_table, events.StartTable), "Expected StartTable event"

    # Skip anything between start table and the first row
    intermediate = []
    while not isinstance(peekable_source.peek(), events.StartRow):
        intermediate.append(next(peekable_source))

    start_row = next(peekable_source)
    assert isinstance(start_row, events.StartRow), "Expected StartRow event"

    headers = []
    while not isinstance(peekable_source.peek(), events.EndRow):
        headers.append(next(peekable_source))

    end_row = next(peekable_source)
    assert isinstance(end_row, events.EndRow), "Expected EndRow event"

    column_headers = _SafeList(c.get('value') for c in headers)

    yield events.StartTable.from_event(start_table, column_headers=column_headers)
    yield from intermediate

    row_index = -1
    col_index = -1
    for e in source:
        if isinstance(e, events.StartRow):
            row_index += 1
            col_index = -1
            yield events.StartRow.from_event(e, row_index=row_index, headers=column_headers)
        elif isinstance(e, events.EndRow):
            yield events.EndRow.from_event(e, row_index=row_index, headers=column_headers)
        elif isinstance(e, events.Cell):
            col_index += 1
            yield events.Cell.from_event(e, column_index=col_index, column_header=column_headers.get(col_index))
        else:
            yield e
