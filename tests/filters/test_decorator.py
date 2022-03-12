from sfdata_stream_parser.events import StartElement, EndElement
from sfdata_stream_parser.filters.generic import streamfilter


def test_simple_decorator():

    @streamfilter
    def filter_func(event):
        return event, EndElement(tag=event.tag)

    assert filter_func.__name__ == 'filter_func'
    stream = filter_func((StartElement(tag='a'), StartElement(tag='b')))
    stream = list(stream)
    assert stream == [
        StartElement(tag='a'),
        EndElement(tag='a'),
        StartElement(tag='b'),
        EndElement(tag='b'),
    ]


def test_decorator_with_args():

    @streamfilter(check=lambda event: event.tag == 'a')
    def filter_func(event):
        return EndElement(tag=event.tag)

    stream = filter_func((StartElement(tag='a'), StartElement(tag='b')))
    stream = list(stream)
    assert stream == [
        EndElement(tag='a'),
    ]
