import functools
from typing import Callable

from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import EventCheck, and_check, type_check, property_check
from sfdata_stream_parser.filters.generic import until_match, first_then_rest


def __collector(func, check: EventCheck, end_check: EventCheck):
    @functools.wraps(func)
    def wrapper(stream, *args, **kwargs):
        stream = peekable(stream)
        while stream:
            yield from until_match(stream, check)
            yield from func(until_match(stream, end_check), *args, **kwargs)

    return wrapper


def collector(*args, **kwargs):
    """
    The collector decorator. It is used to collect data from a stream until a certain event is found.

    This is for example useful to collect groups of events, such as a row from a table, or a nested element in XML.

    A collector has a `check` and `end_check` parameter. The `check` parameter is used to determine when the collector
    should start, and the end_check parameter is used to determine when the collector should stop.

    TODO: Currently the collector passes events prior to start, but this seems a bit inconsistent.
    """
    if len(args) > 0 and isinstance(args[0], Callable):
        return __collector(args[0], *args[1:], **kwargs)
    else:
        def wrapper(func):
            return __collector(func, *args, **kwargs)
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
