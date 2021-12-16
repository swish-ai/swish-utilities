
from typing import List
from cli_util import DipException


class ColumnFilter:
    def __init__(self, file_data, file_column_name, column_name=None):
        if file_column_name in file_data:
            self.values_set = set((str(v) for v in file_data[file_column_name]))
        else:
            raise DipException(f'Provided ids file missing requested id column: {file_column_name}')
        self.column_name = column_name if column_name else file_column_name

    def __call__(self, items):
        if items is None:
            return None
        column_name = self.column_name
        values_set = self.values_set
        return [item for item in items if column_name in item and item[column_name] in values_set]
