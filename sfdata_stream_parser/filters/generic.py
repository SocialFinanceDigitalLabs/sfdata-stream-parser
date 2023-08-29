import functools
from typing import Iterator, Iterable, Callable,Mapping, Any

from sfdata_stream_parser.checks import type_check, EventCheck
from sfdata_stream_parser.events import ParseEvent
from sfdata_stream_parser.function_helpers import filter_caller
from sfdata_stream_parser.functions import *
from sfdata_stream_parser.stream import first_then_rest
from sfdata_stream_parser.types import FilteredValue, EventFilter, FilterErrorHandler


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
                yield from filter_caller(pass_function, event, **kwargs)
            else:
                yield from filter_caller(fail_function, event, **kwargs)
        except Exception as ex:
            yield from filter_caller(error_function, event, ex, **kwargs)


def __event_generator(func, default_args=None, **genargs):
    @functools.wraps(func)
    def wrapper(stream, *_, **kwargs):
        if default_args:
            _kwargs = default_args()
            _kwargs.update(**kwargs)
            kwargs = _kwargs

        if isinstance(stream, ParseEvent):
            stream = (stream,)

        yield from filter_stream(stream, pass_function=func, **genargs, **kwargs)

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


def until_match(iterable: Iterable[events.ParseEvent], check: EventCheck, yield_final=False) -> FilteredValue:
    """
    A stream filter that yields events until the check function returns True.

    If yield_final is False (deafult), then the final event is consumed but not yielded. It can be recovered by
    catching the StopIteration exception. If yield_final is True, then the final event is yielded.

        try:
            while event := next(stream):
                yield event
        except StopIteration as e:
            final_event = e.value
            yield final_event


    :param yield_final:
    :param iterable:
    :param check:
    :return:
    """
    for event in iterable:
        if check(event):
            if yield_final:
                yield event
            return event
        yield event


class GeneratorReturnValueHolder:
    """
    A class that wraps a generator and allows access to the return value of the generator after it has been consumed.

    Attempts at accessing the value before the generator has been consumed will raise a RuntimeError.

    :param stream: The generator to wrap

    Usage:
        generator = [....]
        return_value_holder = GeneratorReturnValueHolder(generator)
        
        # Consume the generator
        list(return_value_holder)

        # Access the return value
        return_value = return_value_holder.value

    """
    def __init__(self, stream):
        self.stream = stream
        self._value = None
        self._consumed = False

    def __iter__(self):
        self._value = yield from self.stream
        self._consumed = True

    def __next__(self):
        try:
            return next(self._value)
        except StopIteration as e:
            self._consumed = True
            self._value = e.value
            raise e

    @property
    def value(self):
        if not self._consumed:
            raise RuntimeError("Generator has not been consumed")
        return self._value


def generator_with_value(func):
    """
    A decorator that wraps a generator function and returns a GeneratorReturnValueHolder object that allows access
    to the return value of the generator after it has been consumed.

    Attempts at accessing the value before the generator has been consumed will raise a RuntimeError.

    :param func:
    :return:
    
    Usage:
        @generator_with_value
        def my_generator():
            yield "value 1"
            yield "value 2"
            return "return value"

        return_value_holder, stream = my_generator()

        # Consume the generator
        list(stream)

        # Access the return value
        return_value = return_value_holder.value
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return_value = GeneratorReturnValueHolder(func(*args, **kwargs))
        return return_value, iter(return_value)

    return wrapper
