from typing import Iterable, Type, Union, Callable
from sfdata_stream_parser import events

EventCheck = Callable[[events.ParseEvent], bool]
OneOrMoreEventTypes = Union[Type[events.ParseEvent], Iterable[Type[events.ParseEvent]]]


def type_check(event_type: OneOrMoreEventTypes = None, permissive=False) -> EventCheck:
    def _check(event: events.ParseEvent) -> bool:
        if event_type is None:
            return permissive
        return isinstance(event, event_type)
    return _check
