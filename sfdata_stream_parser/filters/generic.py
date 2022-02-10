from typing import Iterator, Iterable, Callable, Union

from sfdata_stream_parser import events

EventCheck = Callable[[events.ParseEvent], bool]
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
