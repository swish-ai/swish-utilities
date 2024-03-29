#!/usr/bin/python
from types import SimpleNamespace
import click
import sys
import os
import inspect
#### imports that should be here for py installer ot replaced with --hidden-import
import requests
import logging
import logging.handlers
from flashtext import KeywordProcessor
from src.token_api import get_snow_token
###### end of work around

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import json
import threading
import traceback
from datetime import datetime, timedelta
from cli_util import DipException, dip_option, setup_cli
from dip_help import Help
from src.data_filter import ColumnFilter
from src.extractor_resource import CsvFromJson, DefaultDataProccessor, Extractor
from types import SimpleNamespace
from src.settings import Settings
from src.file import File
import pandas as pd
from pandas.io.parsers import TextFileReader
from pandas.errors import EmptyDataError
from pandas import read_csv, read_excel, DataFrame
from src.cleaner import Masker, TextCleaner, CustomUserFile
from time import time

try:
    from version import VERSION
except:  # NOSONAR
    VERSION = 'dev'


original_input = {}

NOT_CSV_FILE_WARNING = """Provided input file is in csv format while output format is json.
This will cause extra memory usage.
Its strongly recommended to use --output_format csv for csv input."""
# ENUMS
MASK: int = 2
ANONYMIZE: int = 1
DROP: int = 3


mask_data = {'data': (SimpleNamespace(
    user_selected_file_list={},
    custom_tokens_filename_list=[],
    file_objects=[],
    mapping_file_objects=[],
    file_counter=0,
    chunk_size=50
))}


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(VERSION)
    ctx.exit()

def manual_override(val, current_groups, kwargs):
    if val == 'input_dir' and current_groups['extracting'][0] \
                          and current_groups['masking'][0] \
                          and kwargs[val] is None:
        kwargs['input_dir'] = kwargs['output_dir']                         


# Put new parameters here v
@click.command()
@dip_option('--mask', '-m', is_flag=True, help=Help.mask, ns='masking', initial=mask_data)
@dip_option('--extract', '-z', is_flag=True, help=Help.extract, ns="extracting")
@dip_option('--token_get', '-tg', is_flag=True, help=Help.token_get, ns="token_get")
@dip_option('--proccess', '-w', is_flag=True, help=Help.proccess, ns="processing")
@dip_option('--stop_limit', '-l', help=Help.stop_limit, default=1000000000, groups=['extracting'])
@dip_option('--file_limit', '-f', help=Help.file_limit, default=40000000, groups=['extracting'])
@dip_option('--interval', '-i', type=click.IntRange(1, sys.maxsize),
            help=Help.interval, default=24, groups=['extracting'])
@dip_option('--batch_size', '-b', help=Help.batch_size, default=1000, groups=['extracting'])
@dip_option('--parallel', '-x', map_to='parallelism_level', help=Help.parallel, default=1, groups=['extracting'])
@dip_option('--thread_id', '-t', help=Help.thread_id, default=0, groups=['extracting'])
@dip_option('--extension', '-y', help=Help.extension, default='json', groups=['extracting'])
@dip_option('--compress', '-c', help=Help.compress, default=False, groups=['extracting', 'masking'])
@dip_option('--username', '-u', help=Help.username, default='', groups=['extracting', 'token_get'])
@dip_option('--password', '-p', help=Help.password, default='', groups=['extracting', 'token_get'])
@dip_option('--token', '-tk', help=Help.token, default='', groups=['extracting'])
@dip_option('--mfa', '-ms', help=Help.mfa, default='', groups=['token_get'])
@dip_option('--out_token_file', '-ot', help=Help.out_token_file, default='', groups=['token_get'])
@dip_option('--snow_url', '-tc', help=Help.snow_url, default=None, groups=['token_get'])
@dip_option('--start_date', '-s', help=Help.start_date, default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
            groups=['extracting'], swish_default='-')
@dip_option('--end_date', '-e', help=Help.end_date, default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
            groups=['extracting'], swish_default='-')
@dip_option('--url', '-j', help=Help.url, default=None, groups=['extracting'])
@dip_option('--output_format', '-of', help=Help.output_format, default='csv',
            choices=['json', 'csv'], groups=['extracting', 'masking'])
@dip_option('--date_column', '-dc', help=Help.date_column, default='', groups=['extracting'])
@dip_option('--id_list_path', '-q', help=Help.id_list_path, default='', groups=['extracting'])
@dip_option('--id_field_name', '-r', help=Help.id_field_name, default='sys_id', groups=['extracting'])
@dip_option('--data_id_name', '-d', help=Help.data_id_name, default='', groups=['extracting'])
@dip_option('--export_and_mask', '-em', help=Help.export_and_mask, is_flag=True, groups=['extracting'])
@dip_option('--output_dir', '-od', help=Help.output_dir, default='extracting_output', groups=['extracting', 'masking'])
@dip_option('--out_prop_name', '-o', help=Help.out_prop_name, default='documentkey',
            groups=['extracting', 'processing'])
@dip_option('--input_dir', '-id', help=Help.input_dir, default=None, groups=['masking'])
@dip_option('--input_file', '-if', help=Help.input_file, default='', groups=['masking'])
@dip_option('--csv_chunk_size', '-cs', help=Help.csv_chunk_size, default=10000, groups=['masking'])
@dip_option('--mapping_path', '-mp', help=Help.mapping_path, default=None, groups=['masking'])
@dip_option('--custom_token_dir', '-ct', help=Help.custom_token_dir, default='', groups=['masking'])
@dip_option('--important_token_file', '-it', help=Help.important_token_file, default=None, groups=['masking'])
@dip_option('--input_sources', '-is', help=Help.input_sources, default='', groups=['processing'])
@dip_option('--input_encoding', '-ie', help=Help.input_encoding, default='UTF-8', groups=['masking', 'extracting'])
@dip_option('--white_list', '-wl', help=Help.input_encoding, default=[], 
            groups=['masking', 'extracting'], multiple=True)
@dip_option('--out_props_csv_path', '-op', help=Help.out_props_csv_path, default='',
            groups=['extracting', 'processing'])
@dip_option('--pattern', '-pt', help=Help.input_encoding, default=[], 
            groups=['masking', 'extracting'], multiple=True)
@dip_option('--pretty_json', '-pj', is_flag=True, help=Help.pretty_json, groups=['extracting', 'masking'])
@dip_option('--preprocess_patterns', '-pp', is_flag=True, help=Help.preprocess_patterns, groups=['masking'])
@click.option('--version', '-v', help=Help.version, is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
@dip_option('--fix_data', '-fd', is_flag=True, help=Help.fix_data, groups=['masking'])
@dip_option('--skip_bad_lines', '-sb', is_flag=True, help=Help.skip_bad_lines, groups=['masking'])
@dip_option('--set_dtype', '-sd', is_flag=True, help=Help.set_dtype, groups=['masking'])
@click.option('--config', '-cg', help=Help.config, default=None, type=click.STRING)
@click.option('--auth_file', '-af', help=Help.authentication_file, default=None, type=click.STRING)
@dip_option('--all_dates', '-ad', help=Help.debug, is_flag=True, groups=['extracting'])
@click.option('--debug', '-dg', help=Help.all_dates, is_flag=True)
def cli(**kwargs):
    """Utility for ServiceNow data extraction and processing"""
    run_with_args(**kwargs)


def run_with_args(**kwargs):
    try:
        original_input['input_encoding'] = kwargs['input_encoding']
        params = setup_cli(original_input['args'], manual_override, **kwargs)
        start = time()
        exec(params) # NOSONAR
        elapsed = (time() - start)
        click.echo(click.style(f"Utilities version: {VERSION}", fg="cyan"))
        click.echo(click.style(f"Execution time: {timedelta(seconds=elapsed)}", fg="cyan"))
    except Exception as e:
        if kwargs['debug']:
            print(traceback.format_exc())
        click.echo(click.style(f"{e}", fg="red"))
        if "pytest" in sys.modules:
            raise e


def exec_extracting(params, app_settings):
    if params.extracting.enabled:
        if params.extracting.start_date > params.extracting.end_date:
            raise DipException(f'Invalid dates provided')
        try:
            filter_by_column = None
            if params.extracting.id_list_path:
                ids_file = cli_file_read(params.extracting.id_list_path)
                filter_by_column = ColumnFilter(ids_file.data, params.extracting.id_field_name,
                                                params.extracting.data_id_name)
            data_proccessor = None
            if params.extracting.out_props_csv_path:
                data_proccessor = DefaultDataProccessor(params.extracting.out_props_csv_path,
                                                        params.extracting.out_prop_name)
            mask_results = None
            if params.extracting.export_and_mask:
                mask_results = create_masker(params.masking)
            extracting_execute(params, app_settings, filter_by_column, data_proccessor, mask_results)
            if data_proccessor:
                data_proccessor.finalize()
        except Exception as error:
            raise DipException(f'Extracting error, {error}')


def exec(params):
    app_settings = Settings('base')

    try:
        if params.token_get.enabled:
            token = get_snow_token(params.token_get.snow_url, params.token_get.username, params.token_get.password, params.token_get.mfa)
            if params.token_get.out_token_file:
                with open(params.token_get.out_token_file, 'w+') as f:
                    f.write(token)
            else:
                click.echo(token)

        if params.extracting.enabled and params.masking.enabled:
            params.masking.input_dir = params.extracting.output_dir

        exec_extracting(params, app_settings)

        try:
            if params.masking.enabled:
                masking_execute(params.masking, app_settings)
        except Exception as error:
            click.echo(click.style("Error", fg="red") + f"{error}")
            raise DipException(f'Masking error, {error}')

        try:
            if params.processing.enabled:
                processing_execute(params, app_settings)
        except Exception as error:
            raise DipException(f'Processing error, {error}')

    except Exception as error:
        message = f'Execution Error: {error}' + '\n'
        click.echo('\n' + click.style("Error: ", fg="red") + message)
        app_settings.logger.exception(message)

    message = f'Script has FINISHED'
    click.echo(message)

def read_in_chunks(file_object, chunk_size=1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def copy_to_fixed_file(file_path, encoding=None):
    out_file_path = f'{file_path}.temp_swish'
    if os.path.isfile(out_file_path):
        os.remove(out_file_path)
    elcodings = get_encodings_list(encoding)
    for enc in elcodings:
        try:
            with open(file_path, mode='r', encoding=enc) as f:
                with open(out_file_path, mode='a', encoding="utf-8") as of:
                    for piece in read_in_chunks(f):
                        of.write(piece.replace('\r', '<#__swish_r>'))
            return out_file_path
        except UnicodeDecodeError:
            print(f"Faild to open using encoding {enc}")
    raise DipException(f'Failed to read file {file_path}')


def create_masker(mapping_params):
    important_token_file = None
    encodings = get_encodings_list(mapping_params.input_encoding)
    if mapping_params.important_token_file:
        assert os.path.isfile(mapping_params.important_token_file), 'important_token_file is not a valid file name'
        important_token_file = CustomUserFile(mapping_params.important_token_file, encodings=encodings)
    cleaner = TextCleaner(mapping_params.data.custom_tokens_filename_list, important_token_file,
                          encodings=encodings, patterns=mapping_params.pattern,
                          preprocess_patterns=mapping_params.preprocess_patterns)
    custom_token_dir = mapping_params.custom_token_dir
    directory = mapping_params.custom_token_dir
    custom_tokens_filename_list = []
    if directory and os.path.isdir(directory):
        custom_tokens_filename_list = [os.path.join(custom_token_dir, f) for f
                                       in os.listdir(custom_token_dir)
                                       if os.path.isfile(os.path.join(custom_token_dir, f))]

    assert mapping_params.mapping_path and os.path.isfile(mapping_params.mapping_path),\
        '(--mapping_path) mapping file is not a valid file name'
    mapping_file = cli_file_read(mapping_params.mapping_path)

    return Masker(cleaner, mapping_params, mapping_file, custom_tokens_filename_list, 
                  anonymize_value=ANONYMIZE, mask_value=MASK, drop_value=DROP)


def extracting_execute(params, app_settings, filter_by_column, data_proccessor, mask_results):

    # Assert mandatory files/dirs
    try:
        for key in params.extracting.__dict__:
            assert params.extracting.__dict__[key] is not None, f'{key} argument is missing'

        if not params.extracting.__dict__.__contains__('output_dir'):
            directory = 'output'
            params.extracting.output_dir = os.path.join(os.getcwd(), directory)
            if not os.path.isdir(params.extracting.output_dir):
                os.mkdir(directory)

        message = f'Output directory is "{params.extracting.output_dir}'
        app_settings.logger.info(message)
        click.echo(message)

        if params.extracting.parallelism_level > 1:
            extracting_multithreading_execution(params, app_settings, filter_by_column, data_proccessor, mask_results)
        else:
            api_resource = Extractor(params.extracting.start_date, params.extracting.end_date, 0,
                                     app_settings, filter_by_column=filter_by_column,
                                     data_proccessor=data_proccessor, mask_results=mask_results,
                                     all_dates=params.extracting.all_dates, date_column=params.extracting.date_column)
            api_resource.api_extract(params)

            message = f'Total Added: {api_resource.total_added}, Total Failed Approximated: {api_resource.total_failed}'
            app_settings.logger.info(message)
            click.echo(click.style(message, fg="bright_magenta", underline=True))

    except Exception as error:
        message = f'Execution Error: {error}'
        click.echo(click.style(message, fg="red", underline=True))
        app_settings.logger.error(message)

    message = f'Extracting has FINISHED'
    app_settings.logger.info(message)
    click.echo(message)


def processing_execute(params, app_settings):
    if not params.processing.input_sources:
        if os.path.isdir('extracting_output'):
            params.processing.input_sources = 'extracting_output'
        else:
            click.echo('Please specify path to directory containing json files')
            raise DipException('Cant find jsons dir or file')

    data_proccessor = DefaultDataProccessor(params.processing.out_props_csv_path, params.processing.out_prop_name)
    input_sources = params.processing.input_sources.split(',')
    CsvFromJson(app_settings, input_sources, data_proccessor).create_csv()
    data_proccessor.finalize()
    app_settings.logger.info(f"Created csv file with {data_proccessor.size()} entities")


def extracting_multithreading_execution(params, app_settings, filter_by_column, data_proccessor, mask_results):

    thread_list = list()
    total_period = params.extracting.end_date - params.extracting.start_date
    single_period = total_period.total_seconds() / params.extracting.parallelism_level
    batch_start_date = params.extracting.start_date
    batch_end_date = batch_start_date + timedelta(seconds=single_period)

    resources = [None] * params.extracting.parallelism_level
    for thread_id in range(params.extracting.parallelism_level):
        thread_params = params
        thread_params.extracting.start_date = batch_start_date
        thread_params.extracting.end_date = batch_end_date
        thread_params.extracting.thread_id = thread_id

        resources[thread_id] = Extractor(batch_start_date, batch_end_date, thread_id, Settings(str(thread_id)),
                                         filter_by_column=filter_by_column,
                                         data_proccessor=data_proccessor, mask_results=mask_results)

        batch_start_date = batch_start_date + timedelta(seconds=single_period)
        batch_end_date = batch_end_date + timedelta(seconds=single_period)

    for thread_id in range(params.extracting.parallelism_level):
        #
        thread_params = params
        thread = threading.Thread(target=resources[thread_id].api_extract, args=(thread_params,))

        thread.start()
        thread_list.append(thread)

        batch_start_date = batch_start_date + timedelta(seconds=single_period)
        batch_end_date = batch_end_date + timedelta(seconds=single_period)

    total_added = 0
    total_failed = 0
    for index, thread in enumerate(thread_list):
        click.echo(f"Main    : before joining thread {index}")
        thread.join()
        click.echo(f"Main    : thread {index} done")
        total_added += resources[index].total_added
        total_failed += resources[index].total_failed

    message = f'Total Added: {total_added}, Total Failed Approximated: {total_failed}'
    app_settings.logger.info(message)
    click.echo(click.style(message, fg="bright_magenta", underline=True))


def masking_execute(params, app_settings):

    message = f'Masking Started..'
    click.echo(message)
    app_settings.logger.info(message)

    # Assert mandatory files/dirs
    assert os.path.isdir(params.input_dir), 'input file is not a valid directory name'
    assert os.path.isfile(params.mapping_path), 'mapping file is not a valid file name'
    assert os.path.isdir(params.output_dir), 'output_dir is not a valid directory name'
    if params.input_file:
        input_file_path = os.path.join(params.input_dir, params.input_file)
        assert os.path.isfile(input_file_path), 'input_file does not exist'

    # Assert and read important_token_file if exists
    important_token_file = None
    if params.important_token_file != '':
        encodings = get_encodings_list(params.input_encoding)
        assert os.path.isfile(params.important_token_file), 'important_token_file is not a valid file name'
        important_token_file = CustomUserFile(params.important_token_file, encodings=encodings)

    # Assert and read custom_tokens if exist
    if params.custom_token_dir != '':
        assert os.path.isdir(params.custom_token_dir), 'custom_token_file is not a valid directory name'
        custom_tokens_filename_list = [f for f in os.listdir(params.custom_token_dir)
                                       if os.path.isfile(os.path.join(params.custom_token_dir, f))]
        for f in custom_tokens_filename_list:
            params.data.custom_tokens_filename_list.append(os.path.join(params.custom_token_dir, f))
    encodings = get_encodings_list(params.input_encoding)
    params.data.cleaner = TextCleaner(params.data.custom_tokens_filename_list, important_token_file,
                                      encodings=encodings, patterns=params.pattern)

    # Read mandatory files
    mapping_file = cli_file_read(params.mapping_path)
    params.data.mapping_file_objects.append(mapping_file)
    params.data.destination_folder = params.output_dir

    mask_results = create_masker(params)
    input_files = []
    get_all_files(params, input_files)
    for f in input_files:
        input_file = cli_file_read(f, params.input_encoding,
                                                csv_chunk=params.csv_chunk_size,
                                                fix_data=params.fix_data,
                                                set_dtype=params.set_dtype,
                                                skip_bad_lines=params.skip_bad_lines)
        if input_file.ext == 'csv' and params.output_format != 'csv':
            click.echo(click.style(NOT_CSV_FILE_WARNING, fg="yellow"))
        params.data.file_objects.append(input_file)
        cli_file_process(input_file, mask_results, params, app_settings)
    
    remove_swish_temps(params)


def remove_swish_temps(params, cur_dir=None):
    if cur_dir is None:
        cur_dir = ''
    
    files = os.listdir(os.path.join(params.input_dir, cur_dir))
    for f in files:
        p = os.path.join(params.input_dir, cur_dir, f)
        if os.path.isfile(p) and f.lower().endswith('.temp_swish'):
            click.echo(f'Removing temp file {f}')
            os.remove(os.path.join(params.input_dir, cur_dir, f))
        if os.path.isdir(p):
            remove_swish_temps(params, os.path.join(cur_dir, f))


def get_all_files(params, out_res, cur_dir=None):
    if params.input_file:
        out_res.append(os.path.join(params.input_dir, params.input_file))
        return

    if cur_dir is None:
        cur_dir = ''
    files = os.listdir(os.path.join(params.input_dir, cur_dir))
    for f in files:
        p = os.path.join(params.input_dir, cur_dir, f)
        if os.path.isfile(p):
            if f.lower().endswith('.json') or f.lower().endswith('.csv'):
                out_res.append(os.path.join(params.input_dir, cur_dir, f))
            else:
                click.echo(f'unsupported file {f}')
        if os.path.isdir(p):
            get_all_files(params, out_res, os.path.join(cur_dir, f))

def cli_file_process(input_file, masker, params, app_settings):
    click.echo(f'Starting file processing {input_file.filename}')
    f0 = time()
    if input_file.data is not None:
        data = input_file.data
        if not input_file.chunked:
            if isinstance(data, TextFileReader):
                data = [data.read()]
            else:    
                data = [data]
        for chunk in data:
            chunk.fillna(chunk.dtypes.replace({'float64': 0.0, 'O': 'NULL'}), downcast='infer', inplace=True)

            output_data = masker(chunk, no_pd=True, no_output_json=True)
            output_filename = input_file.save_data_to_file(output_data, params.data.destination_folder, params)
        message = f'File processing COMPLETED into: {output_filename} with time:{time() - f0}'
        click.echo(message)
        app_settings.logger.info(message)
    else:
        click.echo(click.style(f"File {input_file.filename} can not be processed", fg="red"))


def load_json_to_file_obj(file_object, encodings):
    encoding = None
    for enc in encodings:
        try:
            f = open(file_object.filename, "r", encoding=enc)
            # Reading from file
            txt = f.read()
            if len(txt) < 1000:
                txt = txt.strip()
            if not txt:
                click.echo(click.style(f"File {file_object.filename} doesn't contain data", fg="red"))
                return
            if encoding:
                txt = txt.encode(encoding)
            data = json.loads(txt)
            encoding = enc
            break
        except Exception as e:
            click.echo(click.style(f"Failed to read file {file_object.filename}" +
                                   f" using encoding {enc}. Error: {e}", fg="yellow"))
    
    if encoding is None:
        click.echo(click.style(f"Failed to read file {file_object.filename}", fg="red"))
        return

    
    # # Checking the json structure
    if 'records' in data:
        file_object.data = DataFrame(data['records'])
    elif 'data' in data:
        file_object.data = DataFrame(data['data'])
    elif isinstance(data, dict) and len(data) == 1:
        file_object.data = DataFrame(next(iter(data.values())))
    else:
        file_object.data = DataFrame(data)

def get_encodings_list(encoding):
    encodings = []
    if encoding is not None:
        encodings.append(encoding.lower())
    for enc in ['utf-8', 'latin-1', 'utf-8-sig']:
        if enc not in encodings:
            encodings.append(enc)
    return encodings


def cli_file_read(filename, encoding=None, csv_chunk=None, fix_data=False,
                  set_dtype=False, skip_bad_lines=False):
    dtype = 'unicode' if set_dtype else None
    on_bad_lines = 'skip' if skip_bad_lines else None
    view_name = os.path.split(filename)[-1]
    obj = {
        "selected": True,
        "filename": filename,
        "ext": filename.split('.')[-1].lower(),
        "view_name": view_name,
        "is_cli": True
        }
    file_object = File(**obj)
    if not os.path.isfile(filename):
        message = f'File {filename} does not exist'
        click.echo(click.style(message, fg="red"))
        raise DipException(message)
    try:
        

        encodings = get_encodings_list(encoding)
        supported_extensions = ['csv', 'json']
        if file_object.ext.lower() not in supported_extensions:
            raise DipException(f'Unsupported file extension {file_object.ext}. Only {supported_extensions} supported')
        try:

            if file_object.ext == 'csv':
                csv_file_name = file_object.filename
                if fix_data:
                    csv_file_name = copy_to_fixed_file(file_object.filename, encoding)
                for enc in encodings:
                    try:
                        if 'on_bad_lines' in inspect.getfullargspec(read_csv).args:
                            if csv_chunk is None:
                                file_object.data = read_csv(csv_file_name, encoding=enc, on_bad_lines=on_bad_lines, dtype=dtype)
                            else:
                                file_object.data = read_csv(csv_file_name, encoding=enc, chunksize=csv_chunk, on_bad_lines=on_bad_lines, dtype=dtype)
                                file_object.chunked = True
                        else:
                            if on_bad_lines:
                                click.echo(click.style(f"Current pandas version does not support on_bad_lines", fg="yellow"))
                            if csv_chunk is None:
                                file_object.data = read_csv(csv_file_name, encoding=enc, dtype=dtype)
                            else:
                                file_object.data = read_csv(csv_file_name, encoding=enc, chunksize=csv_chunk, dtype=dtype)
                                file_object.chunked = True
                        break
                    except EmptyDataError:
                        click.echo(click.style(f"File {file_object.filename} doesn't contain data", fg="yellow"))
                        return file_object
                    except Exception as e:
                        click.echo(click.style(f"Failed to read file {file_object.filename}" +
                                               f" using encoding {enc}, Exception {e}", fg="yellow"))
                if file_object.data is None:
                    click.echo(click.style(f"Failed to read file {file_object.filename}", fg="red"))
            if file_object.ext == 'json':
                load_json_to_file_obj(file_object, encodings)

        except Exception as e:
            click.echo(e.__str__())
            click.echo(click.style(f"Failed to pares input file {file_object.filename}. Error: {e}", fg="red"))

    except Exception as e:
        message = f"Error parsing {filename}. {e}"
        click.echo(click.style(message, fg="red"))
        raise DipException(message)

    return file_object


def main(args):
    global original_input
    original_input['args'] = args
    try:
        cli.main(args)
    except Exception as e:
        click.echo(e)

def cli_exec():
    global original_input
    original_input['args'] = sys.argv
    if len(sys.argv) == 1:
        cli.main(['--help'])
    else:
        cli()

if __name__ == '__main__':
    cli_exec()
