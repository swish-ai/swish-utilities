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
        try:
            output_filename = os.path.join(destination_folder,
                                           self.non_extension_part + '_processed.' + self.ext)
            params.output_filename = output_filename

            # if self.ext == 'xlsx':
            #     output_data.to_excel(r'' + output_filename,
            #                          encoding='utf-8-sig')
            # elif self.ext == 'csv':
            #     try:
            #         output_data.to_csv(r'' + output_filename, index=False, header=True, encoding='utf-8-sig')
            #     except Exception:
            #         try:
            #             output_data.to_csv(r'' + output_filename, index=False, header=True, encoding='utf-8')
            #         except Exception:
            #             output_data.to_csv(r'' + output_filename, index=False, header=True, encoding='latin-1')
            #
            # elif self.ext == 'json':
            try:
                self.write_to_json_file(output_data, params, 'utf-8')
            except Exception as e:
                print(e.__str__(), "\n", "Trying utf-8-sig encoding...")
                self.write_to_json_file(output_data, params, 'utf-8-sig')

        except Exception as e:
            message = f'Error while saving file to: {output_filename}. {e.__str__()}'
            print(message)
            raise DipException(message)

    def write_to_json_file(self, results, params, encoding):

        if params.compress:
            with gzip.open(params.output_filename + '.gz', 'wt', encoding=encoding) as file:
                results.to_json(file, force_ascii=False, orient='records', compression='gzip')
        else:
            with open(params.output_filename, 'w', encoding=encoding) as file:
                results.to_json(file, force_ascii=False, orient='records')
