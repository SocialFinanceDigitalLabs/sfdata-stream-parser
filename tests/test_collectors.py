from collections import Counter
from typing import Iterable, Generator
from unittest.mock import MagicMock

from sfdata_stream_parser.checks import property_check
from sfdata_stream_parser.collectors import collector, collector_check, block_check
from sfdata_stream_parser import events


def _get_events(value: Iterable) -> Generator[events.ParseEvent, None, None]:
    """
    Generate a stream of events from an iterable. The parseevents will have the property value set to the element
    returned from the iterable.
    :param value:
    :return:
    """
    for v in value:
        yield events.ParseEvent(value=v)


class _TestCollector:
    """
    Utility class allowing us to 'remember' events seen by the collector. This is useful for testing the collector.

    All events that are passed to the collector are stored in seen_events.
    """

    def __init__(self, start_value='a', end_value='e', iterations=None, pass_function=None, stop_after=False):
        self._seen_events = seen_events = []

        @collector(
            check=collector_check(property_check(value=start_value), property_check(value=end_value)),
            iterations=iterations,
            pass_function=pass_function,
            stop_after=stop_after,
        )
        def _tester(event):
            seen_events.append(event)
            yield event

        self._tester = _tester

    def __call__(self, *args, **kwargs):
        return self._tester(*args, **kwargs)

    @property
    def seen_events(self):
        return self._seen_events

    @property
    def seen_events_as_text(self):
        return ''.join(map(lambda e: e.value, self.seen_events))


def test_collector_check():
    b = MagicMock()
    a = MagicMock(return_value=b)

    check = collector_check(a, b)
    assert check('anything') is b
    a.assert_called_once()


def test_block_check_default_plain():
    check = block_check()

    end_check = check(events.Cell())
    assert end_check is not None

    assert end_check(events.Cell())
    assert not end_check(events.StartRow())


def test_block_check_default_grouped():
    check = block_check()

    end_check = check(events.StartRow())
    assert end_check is not None

    assert end_check(events.EndRow())
    assert not end_check(events.Cell())
    assert not end_check(events.StartRow())


def test_block_check_default_grouped_nested():
    check = block_check()

    end_check = check(events.StartRow())
    assert end_check is not None

    assert not end_check(events.StartRow())
    assert not end_check(events.Cell())
    assert not end_check(events.EndRow())
    assert end_check(events.EndRow())


def test_single_loop_collect():
    tester = _TestCollector()

    test_string = 'nnnnnabcdennnnnnn'
    stream = _get_events(test_string)
    stream = tester(stream)

    assert ''.join([e.value for e in stream]) == test_string
    assert tester.seen_events_as_text == test_string.replace('n', '')


def test_multi_loop_collect():
    tester = _TestCollector()

    test_string = 'nnnnnabcdennnnnnn' * 5
    stream = _get_events(test_string)
    stream = tester(stream)

    assert ''.join([e.value for e in stream]) == test_string
    assert tester.seen_events_as_text == test_string.replace('n', '')


def test_multi_loop_collect_max_iterations():
    tester = _TestCollector()

    test_string = 'nnnnnabcdennnnnnn' * 5
    stream = _get_events(test_string)
    stream = tester(stream)

    assert ''.join([e.value for e in stream]) == test_string
    assert Counter(tester.seen_events_as_text)['d'] == 5

    # Now limit to two iterations
    tester = _TestCollector(iterations=2)

    stream = _get_events(test_string)
    stream = tester(stream)

    assert ''.join([e.value for e in stream]) == test_string
    assert Counter(tester.seen_events_as_text)['d'] == 2


def test_multi_loop_collect_stop_after():
    tester = _TestCollector(iterations=2, stop_after=True)

    test_string = 'nabcden' * 5
    stream = _get_events(test_string)
    stream = tester(stream)

    observed_string = ''.join([e.value for e in stream])
    assert observed_string == "nabcden" + "nabcde"


def test_collector_with_pass_function():
    tester = _TestCollector(pass_function=lambda e: e.from_event(e, value='-'))

    test_string = 'nnnnnabcdennnnnnn' * 5
    stream = _get_events(test_string)
    stream = tester(stream)

    assert ''.join([e.value for e in stream]) == test_string.replace('n', '-')


def test_collector_stream():
    seen_events = []

    @collector(
        check=collector_check(property_check(value='a'), property_check(value='e')),
        receive_stream=True
    )
    def tester(stream):
        my_events = list(stream)
        seen_events.append(''.join([e.value for e in my_events]))
        yield from my_events

    test_string = 'nnnnnabcdennnnnnn' * 5
    stream = _get_events(test_string)
    stream = tester(stream)

    assert ''.join([e.value for e in stream]) == test_string
    assert len(seen_events) == 5
    for events in seen_events:
        assert events == 'abcde'


def test_collector_event_index():
    @collector(
        check=collector_check(property_check(value='a'), property_check(value='c')),
    )
    def tester(event, event_index):
        yield event.from_event(event, event_index=event_index)

    test_string = 'nnabc' * 2
    stream = _get_events(test_string)
    stream = tester(stream)

    stream = list([(e.value, e.get('event_index')) for e in stream])
    assert stream == [
        ('n', None),
        ('n', None),
        ('a', 0),
        ('b', 1),
        ('c', 2),
        ('n', None),
        ('n', None),
        ('a', 0),
        ('b', 1),
        ('c', 2),
    ]


def test_collector_first_and_last():
    @collector(
        check=collector_check(property_check(value='a'), property_check(value='c')),
    )
    def tester(event, first_event, last_event):
        yield event.from_event(event, first_event=first_event, last_event=last_event)

    test_string = 'nabcn' * 2
    stream = _get_events(test_string)
    stream = tester(stream)

    stream = list([(e.value, e.get('first_event'), e.get('last_event')) for e in stream])
    assert stream == [
        ('n', None, None),
        ('a', True, False),
        ('b', False, False),
        ('c', False, True),
        ('n', None, None),
        ('n', None, None),
        ('a', True, False),
        ('b', False, False),
        ('c', False, True),
        ('n', None, None),
    ]


def test_class_collector():
    class ClassCollector:

        def __init__(self):
            self.seen_events = []
            self.first_seen_events = self.last_seen_events = 0

        @collector(
            check=collector_check(property_check(value='a'), property_check(value='c')),
        )
        def tester(self, event, first_event, last_event):
            self.seen_events.append(event)
            if first_event:
                self.first_seen_events += 1
            if last_event:
                self.last_seen_events += 1
            return event

    c = ClassCollector()

    test_string = 'nabcn' * 5
    stream = _get_events(test_string)
    stream = c.tester(stream)

    stream = list(stream)

    assert c.first_seen_events == 5
