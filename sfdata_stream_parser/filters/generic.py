import functools
from typing import Iterator, Iterable, Callable, Union, Mapping, Any

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check, EventCheck
from sfdata_stream_parser.events import ParseEvent

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
        **kwargs,
) -> Iterable[events.ParseEvent]:
    for event in iterable:
        try:
            if check(event):
                yield from _event_or_iterable(pass_function(event, **kwargs))
            else:
                yield from _event_or_iterable(fail_function(event, **kwargs))
        except Exception as ex:
            yield from _event_or_iterable(error_function(event, ex, **kwargs))


def __event_generator(func, default_args=None, **genargs):
    @functools.wraps(func)
    def wrapper(stream, *args, **kwargs):
        if default_args:
            _kwargs = default_args()
            _kwargs.update(**kwargs)
            kwargs = _kwargs

        if isinstance(stream, ParseEvent):
            stream = [stream]

        yield from filter_stream(stream, *args, pass_function=func, **genargs, **kwargs)

    return wrapper


def streamfilter(arg=None, **kwargs):
    if isinstance(arg, Callable):
        return __event_generator(arg,  **kwargs)
    else:
        def wrapper(func):
            return __event_generator(func,  **kwargs)
        return wrapper


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


def extractor_stream_filter(
        iterable: Iterable[events.ParseEvent],
        extractor: Callable[[events.ParseEvent], Any],
        mapping_functions: Mapping[Any, EventFilter],
):
    """
    A stream filter that calls extractor to fetch a value from an event, and then applies the matching
    mapping_function if found.

    :param iterable:
    :param extractor:
    :param mapping_functions:
    :return:
    """

    def filter_function(event: events.ParseEvent) -> FilteredValue:
        event_value = extractor(event)
        mapping_function = mapping_functions.get(event_value)
        if mapping_function is None:
            yield event
        else:
            yield from mapping_function(event)

    yield from permissive_filter_stream(iterable, filter_function)


def event_attribute_stream_filter(
        iterable: Iterable[events.ParseEvent],
        event_attribute: str,
        mapping_functions: Mapping[str, EventFilter],
) -> Iterable[events.ParseEvent]:
    """
    A stream filter that applies the filter function based on the value of an event attribute.

    :param iterable:
    :param event_attribute:
    :param mapping_functions:
    :return:
    """

    yield from extractor_stream_filter(iterable, lambda x: x.get(event_attribute), mapping_functions)


def check_stream_filter(
        iterable: Iterable[events.ParseEvent],
        mapping_functions: Mapping[EventCheck, EventFilter]
) -> Iterable[events.ParseEvent]:
    """
    A stream filter that applies the filter function based on a custom check function. This is more
    flexible than event_attribute_stream_filter, but not as performant as it performs many more checks.

    :param iterable:
    :param mapping_functions:
    :return:
    """

    def filter_function(event: events.ParseEvent) -> FilteredValue:
        for key, function in mapping_functions.items():
            if key(event):
                yield from function(event)
                return
        yield event

    yield from permissive_filter_stream(iterable, filter_function, type_check(events.Cell))