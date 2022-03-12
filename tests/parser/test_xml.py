from io import StringIO

from sfdata_stream_parser.events import StartElement, EndElement
from sfdata_stream_parser.parser.xml import parse_file


def test_parse_xml():
    stream = list(parse_file(StringIO('<a><b>1<c frog="f">2<d/>3</c></b>4</a>')))
    assert len(stream) == 8

    events = [(type(event), event.as_dict()) for event in stream]

    assert events == [
        (StartElement, {'attrib': {}, 'tag': 'a', 'tail': None, 'text': None}),
        (StartElement, {'attrib': {}, 'tag': 'b', 'tail': '4', 'text': '1'}),
        (StartElement, {'attrib': {'frog': 'f'}, 'tag': 'c', 'tail': None, 'text': '2'}),
        (StartElement, {'attrib': {}, 'tag': 'd', 'tail': '3', 'text': None}),
        (EndElement, {'attrib': {}, 'tag': 'd', 'tail': '3', 'text': None}),
        (EndElement, {'attrib': {'frog': 'f'}, 'tag': 'c', 'tail': None, 'text': '2'}),
        (EndElement, {'attrib': {}, 'tag': 'b', 'tail': '4', 'text': '1'}),
        (EndElement, {'attrib': {}, 'tag': 'a', 'tail': None, 'text': None}),
    ]