import functools
from typing import Callable, Optional

from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.function_helpers import FunctionCaller, event_or_iterable
from sfdata_stream_parser.checks import EventCheck, and_check, type_check, property_check
from sfdata_stream_parser.events import ParseEvent
from sfdata_stream_parser.filters.generic import until_match, pass_event, EventFilter
from sfdata_stream_parser.stream import first_then_rest

CollectorCheck = Callable[[events.ParseEvent], Optional[EventCheck]]


def collector_check(start_check: EventCheck, end_check: EventCheck):
    """
    Based on two normal EventChecks, create a collector check.
    :param start_check:
    :param end_check:
    :return:
    """
    def _start_check(event):
        if start_check(event):
            return end_check
    return _start_check


class _BlockChecker:
    def __init__(self, start_type: ParseEvent = None, end_type: ParseEvent = None):
        self._start_type = start_type
        self._end_type = end_type
        self._counter = 0

    def __call__(self, event: ParseEvent):
        if self._start_type is None:
            self._start_type = type(event)
        if isinstance(event, self._start_type):
            self._counter += 1
            return self._end_check

    def _end_check(self, event):
        if self._end_type is None:
            try:
                self._end_type = self._start_type.end_event
            except AttributeError:
                self._end_type = self._start_type
        if isinstance(event, self._end_type):
            self._counter -= 1
            if self._counter <= 0:
                return True
        elif isinstance(event, self._start_type):
            self._counter += 1
        return False

    @property
    def counter(self):
        return self._counter


def block_check(start_type: ParseEvent = None, end_type: ParseEvent = None):
    """
    Creates a collector check that checks for a nested block of events starting with start_type and ending with end_type.

    If start_type is None, the check will start with the first event in the stream.

    If end_type is None and the start_type (or first event seen) has a .end_event attribute, the check will end with that
    type. Otherwise, the check will end with the same type as the start_type.

    If multiple events of type start_type are seen, then the same number of events of type end_type must be seen before
    the check is satisfied.

    :param start_type:
    :param end_type:
    :return:
    """
    return _BlockChecker(start_type, end_type)


def __pass_events(stream, check, pass_function: EventFilter):
    for event in stream:
        end_check = check(event)
        if end_check is None:
            yield from event_or_iterable(pass_function(event))
        else:
            return end_check, event
    return None, None


def __collector(func, check: CollectorCheck = None, iterations=None, pass_function=None, receive_stream=False,
                stop_after=False):
    """
    See collector.
    """

    if pass_function is None:
        pass_function = pass_event
    if iterations is None:
        iterations = 0

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        caller = FunctionCaller(func, *args, **kwargs)
        stream = caller.stream

        pass_count = 0
        stream = peekable(stream)
        while stream:
            pass_count += 1
            end_check, event = yield from __pass_events(stream, check, pass_function)
            if event is None:
                return
            collected_stream = peekable(first_then_rest(event, until_match(stream, end_check, yield_final=True)))
            if receive_stream:
                yield from caller(collected_stream)
            else:
                for ix, event in enumerate(collected_stream):
                    caller.event_index = ix
                    caller.first_event = ix == 0
                    caller.last_event = not bool(collected_stream)
                    yield from caller(event)
            if 0 < iterations <= pass_count:
                if stop_after:
                    return
                for event in stream:
                    yield from event_or_iterable(pass_function(event))

    return wrapper


def collector(*args, check: CollectorCheck = None, iterations=None, pass_function=None, receive_stream=False,
              stop_after=False):
    """
    The collector decorator. It is used to collect data from a stream between two events.
    A check parameter can be passed to the collector to determine when to start collecting. The
    check is a function that takes a ParseEvent and returns an EventCheck. The collector will pass events
    until the check returns an EventCheck. After that the collector will collecte the events until the
    check passes True, after which it will continue to pass events until the next EventCheck is returned.

    The collector will collect a maximum of iterations times. If iterations is 0, the collector will collect
    indefinitely.

    :param check: The check to use to determine when to start and stop collecting.
    :param iterations: The number of times to repeat the collector.
    :param pass_function: The functions used to pass events before and after the collector. Can be set to explicitly pass or block events.
    :param receive_stream: If True, the stream will be passed to the collector function, otherwise the function will receive individual events.
    :param stop_after: If True, the collector will stop after the last iteration, otherwise it will continue until the end of the stream.
    """
    collector_args = dict(check=check, iterations=iterations, pass_function=pass_function, receive_stream=receive_stream,
                          stop_after=stop_after)

    if len(args) > 0 and isinstance(args[0], Callable):
        return __collector(args[0], **collector_args)
    else:
        def wrapper(func):
            return __collector(func, **collector_args)
        return wrapper


def xml_collector(func):
    """
    A collector that collects events between two XML tags. The start tag is the first tag in the stream, and the end tag
    is the next matching EndElement event with the same tag as the start tag.

    NOTE: This will currently not allow nested elements with the same name.

    TODO: Improve to allow nested elements with the same name assuming a well-formed document.
    """
    @functools.wraps(func)
    def wrapper(stream, *args, **kwargs):
        try:
            start_element = next(stream)
        except StopIteration:
            return tuple()
        assert isinstance(start_element, events.StartElement)

        stream = until_match(
            first_then_rest(start_element, stream),
            and_check(
                type_check(events.EndElement),
                property_check(tag=start_element.tag),
            )
        )

        return func(stream, *args, **kwargs)
    return wrapper
