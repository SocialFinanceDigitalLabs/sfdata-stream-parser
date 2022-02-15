# Tabular Data Stream Parser

![Build and Test](https://github.com/SocialFinanceDigitalLabs/sfdata-stream-parser/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/SocialFinanceDigitalLabs/sfdata-stream-parser/branch/main/graph/badge.svg?token=dqDI49G5qO)](https://codecov.io/gh/SocialFinanceDigitalLabs/sfdata-stream-parser)

Loosely inspired by the Streaming API for XML (StAX), this library provides a Python iterator interface 
for working with tabular documents. Agnostic to the underlying storage format, the library can be used
to generate a stream of 'events' describing the contents of a table.

Typical use-cases for these are data pipeline processing, such as cleaning, filtering and transforming 
tabular data. Being stream-based, the library can be used to process large amounts of data without 
pre-loading the entire contents into memory.

The library depends on the [*more-itertools*][more-itertools] package. This library can be used to build 
complex operations on the stream of events. 

# Events

The following events are defined:

| **Start Event** | **End Event** | **Description**                                                                                                              |
|-----------------|---------------|------------------------------------------------------------------------------------------------------------------------------|
| StartContainer  | EndContainer  | Signifies the start of a container such as a file, or a worksheet within a file. There may be multiple nested containers.    |
| StartTable      | EndTable      | Indicates the scope of a table within the container. This can be an entire file, a worksheet, or a sub-range of a sheet.     |
| StartRow        | EndRow        | Starts and ends an individual row within a table. Rows cannot be nested.                                                     |
| Cell            |               | A cell or column value. Empty columns should also emit cells. Merged cells should emit empty cells for the 'missing' values. Rows can end early if the first few cells are followed only by blanks. |

Take for example the following table:

| Column 1 | Column 2 | Column 3 |
|----------|----------|----------|
| R1C1     | R1C2     | R1C3     |
| R2C1     | R2C2     | R2C3     |

Would result in the following event stream:

* StartContainer (name=README.md)
  * StartTable
    * StartRow(row_index=0)
      * Cell (value=Column 1, column_index=0)
      * Cell (value=Column 2, column_index=1)
      * Cell (value=Column 2, column_index=2)
    * EndRow(row_index=0)
    * StartRow(row_index=1)
      * Cell (value=R1C1, column_index=0)
      * Cell (value=R1C2, column_index=1)
      * Cell (value=R1C3, column_index=2)
    * EndRow(row_index=1)
    * StartRow(row_index=2)
      * Cell (value=R2C1, column_index=0)
      * Cell (value=R2C2, column_index=1)
      * Cell (value=R2C3, column_index=2)
    * EndRow(row_index=2)
  * EndTable
* EndContainer

We have shown this indented for clarity, but the events are emitted in a flat stream.

As we can see, the parser does not have a concept of headers, and the Cells are identified by their position 
in the stream. However, there is a simple 'filter' in *promote_first_row()* that can be used to "pick" the 
first row and use the values as the headers.

So a very simple example of using this tool would be:

```python
from sfdata_stream_parser.parser import parse_csv
from sfdata_stream_parser.filters.column_headers import promote_first_row
with open('filename.csv', newline='') as f:
    stream = promote_first_row(parse_csv(f))
    for event in stream:
        # DO SOMETHING
```

which would result in the following event stream:

* StartContainer (name=README.md)
  * StartTable (headers=[Column 1, Column 2, Column 3])
    * StartRow(row_index=0)
      * Cell (value=R1C1, column_index=0, column_header=Column 1)
      * Cell (value=R1C2, column_index=1, column_header=Column 2)
      * Cell (value=R1C3, column_index=2, column_header=Column 3)
    * EndRow(row_index=0)
    * StartRow(row_index=1)
      * Cell (value=R2C1, column_index=0, column_header=Column 1)
      * Cell (value=R2C2, column_index=1, column_header=Column 2)
      * Cell (value=R2C3, column_index=2, column_header=Column 3)
    * EndRow(row_index=1)
  * EndTable
* EndContainer

Further enriching the stream is as simple as implementing a filter. Here's an example of a filter
that converts cell values from strings into integers where possible:

```python
from typing import Iterable
from sfdata_stream_parser import events

def convert_to_int(stream: Iterable[events.ParseEvent]) -> Iterable[events.ParseEvent]:
    for event in stream:
        if isinstance(event, events.Cell):
            try:
                event = events.Cell.from_event(event, value=int(event.value))
            except ValueError:
                pass
        yield event
```

We provide a shortcut function that allows to call custom functions on specific events:

```python
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import filter_stream, pass_event
from sfdata_stream_parser.checks import type_check

stream = ... # Some event stream

typed_stream = filter_stream(
  stream,
  pass_function=lambda e: events.Cell.from_event(e, value=int(e.value)),
  fail_function=pass_event,
  check=type_check(events.Cell),
)
```
The filter_stream function takes a stream and a set of functions that are called on each event depending on the 
outcome of the check function. If check returns a True for an event, the pass_function is called. If check returns 
False, then the fail_function is called. In this case, we check for a specific event type, so only events of that 
type are passed to the pass_function and the other are just passed straight through unmodified.

This will of course raise an error if the value of cell is not an integer. To handle this we can also 
provide an error function. 

```python
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import filter_stream, ignore_error, pass_event
from sfdata_stream_parser.checks import type_check

stream = ... # Some event stream

typed_stream = filter_stream(
  stream, 
  pass_function=lambda e: events.Cell.from_event(e, value=int(e.value)),
  fail_function=pass_event,
  error_function=ignore_error,
  check=type_check(events.Cell),
)

```

In this case we simply ignore the errors and return the original Cell. If we instead return a None from
either of the fuction, we would completely remove the event from the stream, so this function can also 
work as a filter. 

## Checks and Filters

We have two types of filtering concepts. The first is a check, which is a function that takes an event 
and returns a boolean to indicate whether the event should be passed or blocked. 

The second is a filter, which is a ParseEvent Generator function that takes an event and yields zero, one 
or multiple events.

A filter stream is a combination of a check and two filters, one filters if the event is passed, and
the other if the event is blocked. In addition, the filtered stream can take an error handler that is
invoked if either of the filters raises an error.


[more-itertools]: https://github.com/more-itertools/more-itertools
