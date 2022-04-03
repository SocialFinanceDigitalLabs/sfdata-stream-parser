from sfdata_stream_parser.events import StartElement, EndElement, TextNode, CommentNode, ProcessingInstructionNode
from xml.dom import pulldom


def _coalesce_text_nodes(event_stream):
    last_event = None
    for event in event_stream:
        if isinstance(event, TextNode):
            if last_event:
                last_event = TextNode(text=last_event.text + event.text)
            else:
                last_event = event
        else:
            if last_event:
                yield last_event
            yield event
            last_event = None

    assert last_event is None, "Last event was not emitted"


def parse(source, coalesce=True, element_id=False, attach_node=False, **kwargs):
    """
    Parses an XML document from a file-like object.

    NOTE: Comment nodes are currently not emitted
    :param source:
    :param coalesce: If True, coalesce adjacent text nodes into a single node. Default is True.
    :param element_id: If True, attach the source node's identity to the element node. Default is False.
    :param attach_node: If True, attach the node to the event. Default is False.
    :param kwargs:
    :return:
    """

    def _add_extras(node):
        extras = {}
        if element_id:
            extras['id'] = id(node)
        if attach_node:
            extras['node'] = node
        return extras

    def _generator(stream):
        for event, node in stream:
            if event == pulldom.START_ELEMENT:
                attributes = {}
                for ix in range(node.attributes.length):
                    attr = node.attributes.item(ix)
                    attributes[attr.name] = attr.value
                yield StartElement(tag=node.tagName, attrib=attributes, **_add_extras(node))
            elif event == pulldom.END_ELEMENT:
                yield EndElement(tag=node.tagName, **_add_extras(node))
            elif event == pulldom.CHARACTERS:
                yield TextNode(text=node.data)
            elif event == pulldom.COMMENT:
                yield CommentNode(text=node.data)
            elif event == pulldom.PROCESSING_INSTRUCTION:
                yield ProcessingInstructionNode(name=node.nodeName, text=node.data)
            elif event == pulldom.START_DOCUMENT or event == pulldom.END_DOCUMENT:
                pass
            else:
                raise ValueError('Unknown event: %s' % event)

    event_stream = pulldom.parse(source, **kwargs)
    event_stream = _generator(event_stream)
    if coalesce:
        event_stream = _coalesce_text_nodes(event_stream)
    return event_stream
