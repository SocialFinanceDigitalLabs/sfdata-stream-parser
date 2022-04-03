import inspect
from typing import Callable, Iterable

from sfdata_stream_parser import events
from sfdata_stream_parser.events import ParseEvent
from sfdata_stream_parser.types import FilteredValue


class FunctionCaller:

    def __init__(self, func: Callable, *args, **kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._argspec = inspect.getfullargspec(func)

    def __setattr__(self, key, value):
        if key.startswith("_"):
            super().__setattr__(key, value)
        else:
            self._kwargs[key] = value

    @property
    def stream(self):
        if self.is_method:
            return self._args[1]
        else:
            return self._args[0]

    def __call__(self, event):
        if self.is_method:
            args = (self._args[0], event)
        else:
            args = (event,)

        kwargs = {k: v for k, v in self._kwargs.items() if self.has(k)}
        return event_or_iterable(self._func(*args, **kwargs))

    def has(self, key):
        return _has_arg(key, self._argspec)

    @property
    def is_method(self):
        # I don't like this... but it works
        return self._argspec.args[0] in ["self", "cls"]


def _has_arg(key, argspec):
    return key in argspec.args or key in argspec.kwonlyargs


def filter_caller(func: Callable, event: ParseEvent, exception: Exception = None, **kwargs):
    argspec = inspect.getfullargspec(func)

    event_arg = next(filter(lambda x: x.startswith("ev"), argspec.args), 'event')
    exception_arg = next(filter(lambda x: x.startswith("ex"), argspec.args), 'exception')

    kwargs = {**kwargs, event_arg: event, exception_arg: exception}
    kwargs = {k: v for k, v in kwargs.items() if _has_arg(k, argspec)}

    return event_or_iterable(func(**kwargs))


def event_or_iterable(value: FilteredValue) -> Iterable[events.ParseEvent]:
    if isinstance(value, Iterable):
        yield from value
    elif isinstance(value, events.ParseEvent) or value is not None:
        yield value