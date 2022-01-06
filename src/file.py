import os
import gzip
import json
from pandas import read_csv, read_excel, DataFrame

from cli_util import DipException


class File:
    def __init__(self, filename, ext, selected, view_name, is_cli=False):
        self.view_name = view_name
        self.filename = filename
        self.ext = ext
        self.selected = selected
        self.cleaner = None
        self.chunked = False
        self.current_chunk = 0

        # Extras
        try:
            self.non_extension_part = os.path.split(filename)[-1].split('.')[0]
        except Exception as e:
            print(e.__str__())
            self.non_extension_part = 'NewFile'
            print('failed to extract non extension part from file name')

        # ------ Pandas DataFrame ------ #
        self.data: DataFrame = None
        self.all_data: DataFrame = None
        self.preview_data: DataFrame = None

        self.transform_method_button: dict = {}

    def save_data_to_file(self, output_data, destination_folder, params):
        output_filename = None
        try:
            if params.output_format == 'json':
                output_filename = os.path.join(destination_folder,
                                           self.non_extension_part + '_processed.json')
                params.output_filename = output_filename
                self.save_json_file(output_data, params)
            
            if params.output_format == 'csv':
                output_filename = os.path.join(destination_folder,
                                           self.non_extension_part + '_processed.csv')
                params.output_filename = output_filename
                self.save_csv_file(output_data, params)

        except Exception as e:
            message = f'Error while saving file to: {output_filename}. {e.__str__()}'
            print(message)
            raise DipException(message)
        return output_filename

    def save_json_file(self, output_data, params):
        try:
            self.write_to_json_file(output_data, params, 'utf-8')
        except Exception as e:
            print(e.__str__(), "\n", "Trying utf-8-sig encoding...")
            self.write_to_json_file(output_data, params, 'utf-8-sig')

    def save_csv_file(self, output_data, params):
        try:
            self.write_to_csv_file(output_data, params, 'utf-8')
        except Exception as e:
            print(e.__str__(), "\n", "Trying utf-8-sig encoding...")
            self.write_to_csv_file(output_data, params, 'utf-8-sig')

    def write_to_json_file(self, results, params, encoding):
        indent = None
        if params.pretty_json:
            indent = 4
        if params.compress:
            with gzip.open(params.output_filename + '.gz', 'wt', encoding=encoding) as file:
                results.to_json(file, force_ascii=False, orient='records', compression='gzip', indent=indent)
        else:
            if self.current_chunk == 0:
                with open(params.output_filename, 'w', encoding=encoding) as file:
                    results.to_json(file, force_ascii=False, orient='records', indent=indent)
            else:
                with open(params.output_filename, 'r') as file:
                    jsn = json.load(file) 
                    new_json = json.loads(results.to_json(force_ascii=False,
                                                      orient='records',
                                                      indent=indent), encoding=encoding)
                    jsn = jsn + new_json
                with open(params.output_filename, 'w', encoding=encoding) as file:
                    json.dump(jsn, file)
        self.current_chunk += 1

    def write_to_csv_file(self, results, params, encoding):
        if params.compress and not self.chunked:
            with gzip.open(params.output_filename + '.gz', 'wt', encoding=encoding) as file:
                results.to_csv(file, compression='gzip',index=False)
        else:
            if self.current_chunk == 0:
                results.to_csv(params.output_filename, index=False)
            else:
                results.to_csv(params.output_filename, mode='a', header=False, index=False)
            self.current_chunk += 1
