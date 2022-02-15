import datetime
from unittest.mock import MagicMock

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.types import integer_converter, float_converter, cell_value_converter, _from_excel, \
    date_converter


def cell(value):
    return events.Cell(value=value)


def test_decorator():
    mock_func = MagicMock()
    converter = cell_value_converter(mock_func)

    converter(cell(1))

    mock_func.assert_called_once_with(1)


def test_integers():
    assert integer_converter(cell('1')).value == 1
    assert integer_converter(cell('1.0')).value == 1
    assert isinstance(integer_converter(cell('1.0')).value, int)
    assert integer_converter(cell('0.9998')).value == 1
    assert integer_converter(cell('2.51')).value == 3
    assert integer_converter(cell('100,000.00')).value == 100000
    assert integer_converter(cell('£5')).value == 5

    assert integer_converter(cell('1')).source.value == '1'

    error_cell = integer_converter(cell('a'))
    assert error_cell.value is None
    assert error_cell.error_type == ValueError
    assert error_cell.error_message == "ValueError: could not convert value to integer: 'a'"


def test_floats():
    assert float_converter(cell('1')).value == 1.0
    assert float_converter(cell('1.0')).value == 1.0
    assert isinstance(float_converter(cell('1.0')).value, float)
    assert float_converter(cell('0.9998')).value == 0.9998
    assert float_converter(cell('2.51')).value == 2.51
    assert float_converter(cell('100,000.00')).value == 100000.0
    assert float_converter(cell('£5')).value == 5.0

    assert float_converter(cell('100,000.00')).source.value == '100,000.00'

    error_cell = float_converter(cell('a'))
    assert error_cell.value is None
    assert error_cell.error_type == ValueError
    assert error_cell.error_message == "ValueError: could not convert value to float: 'a'"


def test_from_excel():
    assert _from_excel(42154) == datetime.date(2015, 5, 30)
    assert _from_excel(42154.5) == datetime.datetime(2015, 5, 30, 12, 0, 0)
    assert _from_excel(42154.437675) == datetime.datetime(2015, 5, 30, 10, 30, 15)


def test_date_converter():
    assert date_converter(cell(datetime.date(2015, 7, 13))).value == datetime.date(2015, 7, 13)
    assert date_converter(cell('14/07/2015')).value == datetime.date(2015, 7, 14)
    assert date_converter(cell('2015 - 07 - 14')).value == datetime.date(2015, 7, 14)
    assert date_converter(cell(42200)).value == datetime.date(2015, 7, 15)

    assert date_converter(cell('14/07/15')).error_message == "ValueError: unable to determine format for: '14/07/15'"
