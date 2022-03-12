from sfdata_stream_parser.events import StartElement, EndElement

try:
    from lxml import etree
    __clear_args=dict(keep_tail=True)
except ImportError:
    import xml.etree.ElementTree as etree
    __clear_args={}


def parse_file(source, **kwargs):
    parser = etree.iterparse(source, events=('start', 'end'), **kwargs)
    for action, elem in parser:
        if action == 'start':
            yield StartElement(tag=elem.tag, text=elem.text, tail=elem.tail, attrib=elem.attrib)
        else:
            yield EndElement(tag=elem.tag, text=elem.text, tail=elem.tail, attrib=elem.attrib)
            elem.clear(**__clear_args)
