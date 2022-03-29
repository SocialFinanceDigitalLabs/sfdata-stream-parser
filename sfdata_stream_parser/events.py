
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


class StartContainer(ParseEvent):
    pass


class EndContainer(ParseEvent):
    pass


class StartTable(ParseEvent):
    pass


class EndTable(ParseEvent):
    pass


class StartRow(ParseEvent):
    pass


class EndRow(ParseEvent):
    pass


class Cell(ParseEvent):
    pass


class XmlElement(ParseEvent):
    pass


class StartElement(XmlElement):
    pass

    @property
    def normalised_text(self):
        """
        If the element has a string text property, then return this with whitespace stripped. Otherwise,
        return None.
        """
        text = self.get('text', '')
        if isinstance(text, str):
            return text.strip()


class EndElement(XmlElement):
    pass
