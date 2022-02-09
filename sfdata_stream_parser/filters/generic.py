from collections.abc import Generator
from typing import Any, Iterator, Iterable, Callable


def first_then_rest(first: Any, rest: Iterator[Any]) -> Generator:
    """
    A very simple iterator that yields the first element and then the rest of the iterator. This is useful
    if you have already consumed the first value and want to preserve it.

    :param first:
    :param rest:
    :return:
    """
    yield first
    yield from rest


def conditional_wrapper(
        iterable: Iterable,
        wrapper: Callable[[Iterator], Iterator],
        check: Callable[[Any], bool]
) -> Generator:
    """
    A wrapper for iterators that will delegate to the wrapper function if the check function returns True.

    This can be used for filtering or inserting elements into an iterator, or for modifying certain elements.

    The wrapper can consume the entire iterator, or just the first element. The first element has already been
    consumed by the caller, so if the wrapper wants to preserve the first element it needs to yield it.

    :param iterable:
    :param wrapper:
    :param check:
    :return:
    """
    outer = iter(iterable)
    for outer_value in outer:
        if check(outer_value):
            yield from wrapper(first_then_rest(outer_value, outer))
        else:
            yield outer_value
