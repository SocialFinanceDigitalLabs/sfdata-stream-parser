from typing import Iterator, Iterable

from sfdata_stream_parser import events


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