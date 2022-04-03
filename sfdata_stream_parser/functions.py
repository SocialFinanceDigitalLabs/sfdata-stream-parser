from sfdata_stream_parser import events
from sfdata_stream_parser.types import FilteredValue


def pass_event(event: events.ParseEvent) -> FilteredValue:
    return event


def block_event() -> FilteredValue:
    return tuple()


def raise_error(ex: Exception) -> FilteredValue:
    raise ex


def ignore_error(event: events.ParseEvent) -> FilteredValue:
    return event


def skip_error() -> FilteredValue:
    return ()
