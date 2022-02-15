from typing import Iterable, Type, Union, Callable
from sfdata_stream_parser import events

EventCheck = Callable[[events.ParseEvent], bool]
OneOrMoreEventTypes = Union[Type[events.ParseEvent], Iterable[Type[events.ParseEvent]]]


def and_check(*args: EventCheck) -> EventCheck:
    def _check(event: events.ParseEvent) -> bool:
        for check in args:
            if not check(event):
                return False
        return True
    return _check


def or_check(*args: EventCheck) -> EventCheck:
    def _check(event: events.ParseEvent) -> bool:
        for check in args:
            if check(event):
                return True
        return False
    return _check


def type_check(event_type: OneOrMoreEventTypes = None, permissive=False) -> EventCheck:
    def _check(event: events.ParseEvent) -> bool:
        if event_type is None:
            return permissive
        return isinstance(event, event_type)
    return _check
