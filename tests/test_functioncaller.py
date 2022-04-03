import types
from functools import wraps

from sfdata_stream_parser import events
from sfdata_stream_parser.function_helpers import FunctionCaller


def test_simple():

    def my_func1(event):
        assert isinstance(event, events.ParseEvent)
        return event

    caller = FunctionCaller(my_func1)
    assert not caller.is_method

    stream = caller(events.ParseEvent(value='hello'))
    assert isinstance(stream, types.GeneratorType)
    assert next(stream).value == 'hello'
    assert len(list(stream)) == 0

    def my_func2(event):
        assert isinstance(event, events.ParseEvent)
        yield event

    stream = FunctionCaller(my_func2)(events.ParseEvent(value='hello'))
    assert isinstance(stream, types.GeneratorType)
    assert next(stream).value == 'hello'
    assert len(list(stream)) == 0

    def my_func3(event):
        assert isinstance(event, events.ParseEvent)
        yield event
        yield event

    stream = FunctionCaller(my_func3)(events.ParseEvent(value='hello'))
    assert isinstance(stream, types.GeneratorType)
    assert next(stream).value == 'hello'
    assert next(stream).value == 'hello'
    assert len(list(stream)) == 0

    def my_func4(_):
        return

    stream = FunctionCaller(my_func4)(events.ParseEvent(value='hello'))
    assert isinstance(stream, types.GeneratorType)
    assert len(list(stream)) == 0


def test_decorated_method():
    def decorator(func):
        def wrapped(*args, **kwargs):
            caller = FunctionCaller(func, *args, **kwargs)
            return caller(caller.stream)
        return wrapped

    class MyClass:
        __check__ = 'be2be2'

        @decorator
        def my_method(self, event):
            yield event.from_event(event, check_value=self.__check__)

    my_obj = MyClass()
    stream = my_obj.my_method(events.ParseEvent(value='hello'))

    assert isinstance(stream, types.GeneratorType)

    next_event = next(stream)
    assert isinstance(next_event, events.ParseEvent)
    assert next_event.value == 'hello'
    assert next_event.check_value == 'be2be2'

    assert len(list(stream)) == 0


def test_has_property():
    def func_with_props(event, prop1, prop3, prop5="Hello", *, prop6="World"):
        return event.from_event(event, prop1=prop1, prop3=prop3, prop5=prop5, prop6=prop6)

    caller = FunctionCaller(func_with_props)

    assert caller.has('prop1')
    assert not caller.has('prop2')
    assert caller.has('prop3')
    assert not caller.has('prop4')
    assert caller.has('prop5')
    assert caller.has('prop6')


def test_pass_properties():
    def func_with_props(event, prop1, prop3, prop5="Hello", *, prop6="World"):
        return event.from_event(event, prop1=prop1, prop3=prop3, prop5=prop5, prop6=prop6)

    kwargs = dict(prop1='value1', prop3='value3', prop5='value5', prop6='value6')

    caller = FunctionCaller(func_with_props, **kwargs)
    source_event = events.ParseEvent(value='initial')
    stream = caller(source_event)

    next_event = next(stream)
    assert next_event.as_dict() == dict(value=source_event.value, prop1='value1', prop3='value3', prop5='value5',
                                        prop6='value6', source=source_event)

    kwargs = dict(prop3='value3', prop5='value5', prop6='value6', weird_prop='what?')
    caller = FunctionCaller(func_with_props, **kwargs)
    caller.prop1 = 'VALUE1'
    caller.prop3 = 'VALUE3'

    source_event = events.ParseEvent(value='initial')
    stream = caller(source_event)

    next_event = next(stream)
    assert next_event.as_dict() == dict(value=source_event.value, prop1='VALUE1', prop3='VALUE3', prop5='value5',
                                        prop6='value6', source=source_event)
