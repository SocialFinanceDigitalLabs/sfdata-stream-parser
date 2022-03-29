from typing import Iterable, Type, Union, Callable
from sfdata_stream_parser import events

EventCheck = Callable[[events.ParseEvent], bool]
OneOrMoreEventTypes = Union[Type[events.ParseEvent], Iterable[Type[events.ParseEvent]]]


def and_check(*args: EventCheck) -> EventCheck:
    """
    Combines multiple checks into one using the logical AND operator.
    """
    def _check(event: events.ParseEvent) -> bool:
        for check in args:
            if not check(event):
                return False
        return True
    return _check


def or_check(*args: EventCheck) -> EventCheck:
    """
    Combines multiple checks into one using the logical OR operator.
    """
    def _check(event: events.ParseEvent) -> bool:
        for check in args:
            if check(event):
                return True
        return False
    return _check


def type_check(event_type: OneOrMoreEventTypes = None, permissive=False) -> EventCheck:
    """
    Checks that the event is of the given type.
    """
    def _check(event: events.ParseEvent) -> bool:
        if event_type is None:
            return permissive
        return isinstance(event, event_type)
    return _check


def property_check(**kwargs) -> EventCheck:
    """
    Checks that the event has the given properties with the given values.

    TODO: Expand to allow django_filters style properties, e.g. age__gt=18 or band__icontains='foo'
    """
    def _check(event: events.ParseEvent) -> bool:
        for key, value in kwargs.items():
            if getattr(event, key, None) != value:
                return False
        return True

    return _check
