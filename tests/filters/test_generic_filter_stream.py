from unittest.mock import MagicMock

import pytest

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import filter_stream, FilteredValue, skip_error, ignore_error, pass_event


@pytest.fixture
def stream():
    return (
        events.StartContainer(),
        events.StartTable(),
        events.EndTable(),
        events.EndContainer(),
    )


def test_pass_all(stream):
    event_list = list(filter_stream(stream, check=lambda event: True))
    assert len(event_list) == 4


def test_fail_all(stream):
    event_list = list(filter_stream(stream, check=lambda event: False))
    assert len(event_list) == 0


def test_pass_function(stream):
    pass_function = MagicMock()
    fail_function = MagicMock()
    error_function = MagicMock()

    list(filter_stream(
        stream,
        check=lambda event: True,
        pass_function=pass_function,
        fail_function=fail_function,
        error_function=error_function,
    ))

    assert pass_function.call_count == 4
    assert fail_function.call_count == 0
    assert error_function.call_count == 0


def test_fail_function(stream):
    pass_function = MagicMock()
    fail_function = MagicMock()
    error_function = MagicMock()

    list(filter_stream(
        stream,
        check=lambda event: False,
        pass_function=pass_function,
        fail_function=fail_function,
        error_function=error_function,
    ))

    assert pass_function.call_count == 0
    assert fail_function.call_count == 4
    assert error_function.call_count == 0


def test_error_function(stream):
    pass_function = MagicMock()
    fail_function = MagicMock()
    error_function = MagicMock()

    list(filter_stream(
        stream,
        check=lambda event: event == 1/0,
        pass_function=pass_function,
        fail_function=fail_function,
        error_function=error_function,
    ))

    assert pass_function.call_count == 0
    assert fail_function.call_count == 0
    assert error_function.call_count == 4


def test_filter_stream_with_error(stream):
    def _pass_func(event):
        if isinstance(event, events.EndTable):
            raise ValueError('EndTable not allowed for this test')
        yield event

    with pytest.raises(ValueError) as excinfo:
        list(filter_stream(stream, pass_function=_pass_func))

    assert 'EndTable not allowed for this test' in str(excinfo.value)


def test_filter_stream_with_error_function(stream):
    def _pass_func(event):
        if isinstance(event, events.EndTable):
            raise ValueError('EndTable not allowed for this test')
        yield event

    event_list = list(filter_stream(stream, pass_function=_pass_func, error_function=skip_error))
    assert len(event_list) == 3

    def _error_func_returning_none(*args) -> FilteredValue:
        return None

    event_list = list(filter_stream(stream, pass_function=_pass_func, error_function=_error_func_returning_none))
    assert len(event_list) == 3

    event_list = list(filter_stream(stream, pass_function=_pass_func, error_function=ignore_error))
    assert len(event_list) == 4

    def _error_func_returning_multiple(event, _) -> FilteredValue:
        yield event
        yield event

    event_list = list(filter_stream(stream, pass_function=_pass_func, error_function=_error_func_returning_multiple))
    assert len(event_list) == 5


def test_modify_every_event(stream):
    sequence = {}

    def _pass_func(event):
        seq = sequence['v'] = sequence.get('v', 0) + 1
        yield type(event).from_event(event, value=seq)

    event_list = list(filter_stream(stream, pass_function=_pass_func))
    assert [e.value for e in event_list] == [1, 2, 3, 4]

    sequence['v'] = 0
    event_list = list(filter_stream(stream, pass_function=_pass_func, check=type_check(events.EndTable)))
    assert [e.get('value', -1) for e in event_list] == [1]  # For this filter we block non matches

    sequence['v'] = 0
    event_list = list(filter_stream(stream, pass_function=_pass_func, fail_function=pass_event, check=type_check(events.EndTable)))
    assert [e.get('value', -1) for e in event_list] == [-1, -1, 1, -1]  # For this filter we block non matches
