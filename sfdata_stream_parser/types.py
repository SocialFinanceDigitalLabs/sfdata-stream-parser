from typing import Iterator, Callable, Union

from sfdata_stream_parser import events

FilteredValue = Union[events.ParseEvent, Iterator[events.ParseEvent]]
EventFilter = Callable[[events.ParseEvent], FilteredValue]
FilterErrorHandler = Callable[[events.ParseEvent, Exception], FilteredValue]

