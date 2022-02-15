from typing import Iterator, Iterable, Callable, Union, Mapping

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check, EventCheck

FilteredValue = Union[events.ParseEvent, Iterator[events.ParseEvent]]
EventFilter = Callable[[events.ParseEvent], FilteredValue]
FilterErrorHandler = Callable[[events.ParseEvent, Exception], FilteredValue]


def _event_or_iterable(value: FilteredValue) -> Iterable[events.ParseEvent]:
    if isinstance(value, Iterable):
        yield from value
    elif isinstance(value, events.ParseEvent) or value is not None:
        yield value


def first_then_rest(first: events.ParseEvent, rest: Iterator[events.ParseEvent]) -> Iterable[events.ParseEvent]:
    """
    A very simple iterator that yields the first element and then the rest of the iterator. This is useful
    if you have already consumed the first value and want to preserve it.

    :param first:
    :param rest:
    :return:
    """
    yield first
    yield from rest


def conditional_wrapper(
        iterable: Iterable[events.ParseEvent],
        wrapper: Callable[[Iterator], Iterator],
        check: EventCheck
) -> Iterable[events.ParseEvent]:
    """
    A wrapper for iterators that will delegate to the wrapper function if the check function returns True.

    This can be used for filtering or inserting elements into an iterator, or for modifying certain elements.

    The wrapper can consume the entire iterator, or just the first element. The first element has already been
    consumed by the caller, so if the wrapper wants to preserve the first element it needs to yield it.

    :param iterable:
    :param wrapper:
    :param check:
    :return:
    """
    outer = iter(iterable)
    for outer_value in outer:
        if check(outer_value):
            yield from wrapper(first_then_rest(outer_value, outer))
        else:
            yield outer_value


def pass_event(event: events.ParseEvent) -> FilteredValue:
    return event


def block_event(_) -> FilteredValue:
    return tuple()


def raise_error(_, ex: Exception) -> FilteredValue:
    raise ex


def ignore_error(event: events.ParseEvent, _) -> FilteredValue:
    return event


def skip_error(*args) -> FilteredValue:
    return ()


def filter_stream(
        iterable: Iterable[events.ParseEvent],
        check: EventCheck = lambda x: True,
        pass_function: EventFilter = pass_event,
        fail_function: EventFilter = block_event,
        error_function: FilterErrorHandler = raise_error,
) -> Iterable[events.ParseEvent]:
    for event in iterable:
        try:
            if check(event):
                yield from _event_or_iterable(pass_function(event))
            else:
                yield from _event_or_iterable(fail_function(event))
        except Exception as ex:
            yield from _event_or_iterable(error_function(event, ex))


def permissive_filter_stream(
        iterable: Iterable[events.ParseEvent],
        function: EventFilter,
        check: EventCheck = lambda x: True,
) -> Iterable[events.ParseEvent]:
    """
    A short-hand for a filter stream that applies the function to all events that pass the check. Otherwise
    events pass through unchanged.

    :param iterable:
    :param function:
    :param check:
    :return:
    """

    for event in iterable:
        if check(event):
            yield from function(event)
        else:
            yield event


def cell_header_stream_filter(
        iterable: Iterable[events.ParseEvent],
        mapping_functions: Mapping[str, EventFilter]
) -> Iterable[events.ParseEvent]:
    """
    A stream filter that applies the filter function based on the value of the cell header.

    :param iterable:
    :param mapping_functions:
    :return:
    """

    def filter_function(event: events.ParseEvent) -> FilteredValue:
        column_header = event.get('column_header')
        mapping_function = mapping_functions.get(column_header)
        if mapping_function is None:
            yield event
        else:
            yield from mapping_function(event)

    yield from permissive_filter_stream(iterable, filter_function, type_check(events.Cell))
