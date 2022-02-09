import csv
from sfdata_stream_parser.events import *


def parse_csv(csvfile, name=None, table_name=None, **csvargs):
    yield StartContainer(name=name)
    yield StartTable(name=table_name or name)
    for row_ix, row in enumerate(csv.reader(csvfile, **csvargs)):
        yield StartRow(row_index=row_ix)
        for col_ix, cell in enumerate(row):
            yield Cell(value=cell, column_index=col_ix)
        yield EndRow(row_index=row_ix)
    yield EndTable(name=table_name or name)
    yield EndContainer(name=name)
