from io import StringIO
from xml.dom import minidom

from sfdata_stream_parser.events import StartElement, EndElement, TextNode, CommentNode, ProcessingInstructionNode
from sfdata_stream_parser.parser.xml import parse


def test_parse_xml():
    stream = list(parse(StringIO('<a><?PITarget PIContent?><b>1<c frog="f">2a<!-- yeah -->2b<d/>3</c></b>4</a>')))

    events = [(type(event), event.as_dict()) for event in stream]

    assert events == [
        (StartElement, {'tag': 'a', 'attrib': {}}),
        (ProcessingInstructionNode, {'name': 'PITarget', 'text': 'PIContent'}),
        (StartElement, {'tag': 'b', 'attrib': {}}),
        (TextNode, {'text': '1'}),
        (StartElement, {'tag': 'c', 'attrib': {'frog': 'f'}}),
        (TextNode, {'text': '2a2b'}),
        (StartElement, {'tag': 'd', 'attrib': {}}),
        (EndElement, {'tag': 'd'}),
        (TextNode, {'text': '3'}),
        (EndElement, {'tag': 'c'}),
        (EndElement, {'tag': 'b'}),
        (TextNode, {'text': '4'}),
        (EndElement, {'tag': 'a'}),
    ]


def test_coalesce():
    xml = '<a>b<!-- yeah -->c</a>'

    stream = parse(StringIO(xml), coalesce=True)
    events = [(type(event), event.as_dict()) for event in stream]
    assert events == [
        (StartElement, {'tag': 'a', 'attrib': {}}),
        (TextNode, {'text': 'bc'}),
        (EndElement, {'tag': 'a'}),
    ]

    stream = parse(StringIO(xml), coalesce=False)
    events = [(type(event), event.as_dict()) for event in stream]
    assert events == [
        (StartElement, {'tag': 'a', 'attrib': {}}),
        (TextNode, {'text': 'b'}),
        (TextNode, {'text': 'c'}),
        (EndElement, {'tag': 'a'}),
    ]


def test_element_id():
    xml = '<a><b><c/></b><d/></a>'
    stream = parse(StringIO(xml), element_id=True)
    stream = list(stream)  # We are going to consume the stream twice, so we need to cache it

    start_events = filter(lambda e: isinstance(e, StartElement), stream)
    end_events = filter(lambda e: isinstance(e, EndElement), stream)

    start_ids = {e.tag: e.id for e in start_events}
    end_ids = {e.tag: e.id for e in end_events}

    assert start_ids == end_ids
    assert len(start_ids) == 4


def test_attach_node():
    xml = '<a><b><c/></b><d/></a>'
    stream = parse(StringIO(xml), attach_node=True)
    stream = list(stream)  # We are going to consume the stream twice, so we need to cache it

    start_events = list(filter(lambda e: isinstance(e, StartElement), stream))
    end_events = list(filter(lambda e: isinstance(e, EndElement), stream))

    start_ids = {e.tag: e.node for e in start_events}
    end_ids = {e.tag: e.node for e in end_events}

    assert start_ids == end_ids
    assert len(start_ids) == 4

    for e in start_events:
        assert isinstance(e.node, minidom.Element)
