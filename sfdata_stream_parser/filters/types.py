import datetime
import re
from math import floor

from sfdata_stream_parser import events
from sfdata_stream_parser.events import Cell


def cell_value_converter(func):
    def wrapper(cell: Cell, **kwargs):
        value = cell.get('value')
        if value is None:
            return None
        else:
            try:
                return events.Cell.from_event(cell, value=func(value, **kwargs))
            except Exception as e:
                return events.Cell.from_event(cell, value=None, error_type=type(e), error_message=str(e))

    return wrapper


@cell_value_converter
def integer_converter(value):
    try:
        return round(_float_converter(value))
    except ValueError:
        raise ValueError(f"ValueError: could not convert value to integer: '{value}'")


@cell_value_converter
def float_converter(value):
    try:
        return _float_converter(value)
    except ValueError:
        raise ValueError(f"ValueError: could not convert value to float: '{value}'")


def _float_converter(value):
    try:
        return float(value)
    except ValueError:
        pass

    if isinstance(value, str):
        value = re.sub(r'[^\d.]', '', value)

    return float(value)


@cell_value_converter
def date_converter(value):
    if isinstance(value, datetime.datetime):
        return value.date

    if isinstance(value, datetime.date):
        return value

    if isinstance(value, float):
        return _from_excel(floor(value))

    if isinstance(value, int):
        return _from_excel(value)

    if isinstance(value, str):
        parts = re.split(r'[^\d]+', value.strip(), 3)
        parts = [int(p) for p in parts]
        if parts[0] > 31:
            return datetime.date(parts[0], parts[1], parts[2])
        elif parts[2] > 31:
            return datetime.date(parts[2], parts[1], parts[0])
        else:
            raise ValueError(f"ValueError: unable to determine format for: '{value}'")

    raise ValueError(f"ValueError: unknown date value: '{value}'")


def _from_excel(value):
    dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=int(value))
    if isinstance(value, int):
        return dt.date()
    else:
        hours, hour_seconds = divmod(value % 1*24, 1)
        minutes, seconds = divmod(hour_seconds*60, 1)
        seconds *= 60
        return dt.replace(hour=int(hours), minute=int(minutes), second=int(seconds))
