
class ParseEvent:
    def __init__(self, **kwargs):
        self._args = kwargs

    def __getattr__(self, item):
        try:
            return self._args[item]
        except KeyError as e:
            raise AttributeError from e

    def get(self, key, default=None):
        return self._args.get(key, default)

    @classmethod
    def from_event(cls, event, **kwargs):
        return cls(**{**event.as_dict(), **kwargs, 'source': event})

    def as_dict(self):
        return self._args

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()


class EndContainer(ParseEvent):
    pass


class StartContainer(ParseEvent):
    end_event = EndContainer
    pass


class EndTable(ParseEvent):
    pass


class StartTable(ParseEvent):
    end_event = EndTable
    pass


class EndRow(ParseEvent):
    pass


class StartRow(ParseEvent):
    end_event = EndRow
    pass


class Cell(ParseEvent):
    pass


class XmlEvent(ParseEvent):
    pass


class EndElement(XmlEvent):
    def __init__(self, **kwargs):
        assert 'tag' in kwargs, "A tag property is required"
        super().__init__(**kwargs)


class StartElement(XmlEvent):
    end_event = EndElement

    def __init__(self, **kwargs):
        assert 'tag' in kwargs, "A tag property is required"
        super().__init__(**kwargs)


class ProcessingInstructionNode(XmlEvent):
    def __init__(self, **kwargs):
        assert 'text' in kwargs, "A text property is required"
        super().__init__(**kwargs)


class CommentNode(XmlEvent):
    def __init__(self, **kwargs):
        assert 'text' in kwargs, "A text property is required"
        super().__init__(**kwargs)


class TextNode(XmlEvent):

    def __init__(self, **kwargs):
        assert 'text' in kwargs, "A text property is required"
        super().__init__(**kwargs)

