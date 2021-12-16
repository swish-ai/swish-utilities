#!/usr/bin/python
from types import SimpleNamespace
import click
import sys
import os
import json
import threading

from datetime import datetime, timedelta
from cli_util import DipException, dip_option, setup_cli
from dip_help import Help
from src.data_filter import ColumnFilter
from src.extractor_resource import CsvFromJson, DefaultDataProccessor, Extractor
from types import SimpleNamespace
from src.settings import Settings
from src.file import File
from pandas import read_csv, read_excel, DataFrame
from src.cleaner import Masker, TextCleaner, CustomUserFile
from time import time

try:
    from version import VERSION
except:  # NOSONAR
    VERSION = 'dev'


# ENUMS
MASK: int = 2
ANONYMIZE: int = 1
NONE_ACTION: int = 3

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


# Put new parameters here v
@click.command()
@dip_option('--mask', '-m', is_flag=True, help=Help.mask, ns='masking', initial=mask_data)
@dip_option('--extract', '-z', is_flag=True, help=Help.extract, ns="extracting")
@dip_option('--proccess', '-w', is_flag=True, help=Help.proccess, ns="processing")
@dip_option('--stop_limit', '-l', help=Help.stop_limit, default=1000000000, groups=['extracting'])
@dip_option('--file_limit', '-f', help=Help.file_limit, default=1000000, groups=['extracting'])
@dip_option('--interval', '-i', type=click.IntRange(1, sys.maxsize),
            help=Help.interval, default=24, groups=['extracting'])
@dip_option('--batch_size', '-b', help=Help.batch_size, default=1000, groups=['extracting'])
@dip_option('--parallel', '-x', map_to='parallelism_level', help=Help.parallel, default=1, groups=['extracting'])
@dip_option('--thread_id', '-t', help=Help.thread_id, default=0, groups=['extracting'])
@dip_option('--extension', '-y', help=Help.extension, default='json', groups=['extracting'])
@dip_option('--compress', '-c', help=Help.compress, default=False, groups=['extracting', 'masking'])
@dip_option('--username', '-u', help=Help.username, default=None, groups=['extracting'])
@dip_option('--password', '-p', help=Help.password, default='', groups=['extracting'])
@dip_option('--start_date', '-s', help=Help.start_date, default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
            groups=['extracting'])
@dip_option('--end_date', '-e', help=Help.end_date, default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
            groups=['extracting'])
@dip_option('--url', '-j', help=Help.url, default=None, groups=['extracting'])
@dip_option('--id_list_path', '-q', help=Help.id_list_path, default='', groups=['extracting'])
@dip_option('--id_field_name', '-r', help=Help.id_field_name, default='sys_id', groups=['extracting'])
@dip_option('--data_id_name', '-d', help=Help.data_id_name, default='', groups=['extracting'])
@dip_option('--export_and_mask', '-em', help=Help.export_and_mask, is_flag=True, groups=['extracting'])
@dip_option('--output_dir', '-od', help=Help.output_dir, default='extracting_output', groups=['extracting', 'masking'])
@dip_option('--out_prop_name', '-o', help=Help.out_prop_name, default='documentkey',
            groups=['extracting', 'processing'])
@dip_option('--input_dir', '-id', help=Help.input_dir, default=None, groups=['masking'])
@dip_option('--mapping_path', '-mp', help=Help.mapping_path, default=None, groups=['masking'])
@dip_option('--custom_token_dir', '-ct', help=Help.custom_token_dir, default='', groups=['masking'])
@dip_option('--important_token_file', '-it', help=Help.important_token_file, default=None, groups=['masking'])
@dip_option('--input_sources', '-is', help=Help.input_sources, default='', groups=['processing'])
@dip_option('--out_props_csv_path', '-op', help=Help.out_props_csv_path, default='',
            groups=['extracting', 'processing'])
@click.option('--version', '-v', help=Help.version, is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
@click.option('--config', '-cg', help=Help.config, default=None, type=click.STRING)
@click.option('--auth_file', '-af', help=Help.authentication_file, default=None, type=click.STRING)
def cli(**kwargs):
    """Utility for ServiceNow data extraction and processing"""
    params = setup_cli(**kwargs)
    start = time()
    exec(params)
    elapsed = (time() - start)
    click.echo(click.style(f"Execution time: {timedelta(seconds=elapsed)}", fg="cyan"))


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


def create_masker(mapping_params):
    important_token_file = None
    if mapping_params.important_token_file:
        assert os.path.isfile(mapping_params.important_token_file), 'important_token_file is not a valid file name'
        important_token_file = CustomUserFile(mapping_params.important_token_file)
    cleaner = TextCleaner(mapping_params.data.custom_tokens_filename_list, important_token_file)
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

    return Masker(cleaner, mapping_file, custom_tokens_filename_list, anonymize_value=ANONYMIZE, mask_value=MASK)


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
                                     data_proccessor=data_proccessor, mask_results=mask_results)
            api_resource.api_extract(params)

            message = f'Total Added: {api_resource.total_added}, Total Failed Approximated: {api_resource.total_failed}'
            app_settings.logger.info(message)
            click.echo(message)

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
    click.echo(message)


def masking_execute(params, app_settings):

    message = f'Masking Started..'
    click.echo(message)
    app_settings.logger.info(message)

    # Assert mandatory files/dirs
    assert os.path.isdir(params.input_dir), 'input file is not a valid directory name'
    assert os.path.isfile(params.mapping_path), 'mapping file is not a valid file name'
    assert os.path.isdir(params.output_dir), 'output_dir is not a valid directory name'

    # Assert and read important_token_file if exists
    important_token_file = None
    if params.important_token_file != '':
        assert os.path.isfile(params.important_token_file), 'important_token_file is not a valid file name'
        important_token_file = CustomUserFile(params.important_token_file)

    # Assert and read custom_tokens if exist
    if params.custom_token_dir != '':
        assert os.path.isdir(params.custom_token_dir), 'custom_token_file is not a valid directory name'
        custom_tokens_filename_list = [f for f in os.listdir(params.custom_token_dir)
                                       if os.path.isfile(os.path.join(params.custom_token_dir, f))]
        for f in custom_tokens_filename_list:
            params.data.custom_tokens_filename_list.append(os.path.join(params.custom_token_dir, f))

    params.data.cleaner = TextCleaner(params.data.custom_tokens_filename_list, important_token_file)

    # Read mandatory files
    mapping_file = cli_file_read(params.mapping_path)
    params.data.mapping_file_objects.append(mapping_file)
    params.data.destination_folder = params.output_dir

    mask_results = create_masker(params)
    # Read input files and execute
    input_files = [f for f in os.listdir(params.input_dir) if os.path.isfile(os.path.join(params.input_dir, f))]
    for f in input_files:
        input_file = cli_file_read(os.path.join(params.input_dir, f))
        params.data.file_objects.append(input_file)
        cli_file_process(input_file, mask_results, params, app_settings)


def cli_file_process(input_file, masker, params, app_settings):

    f0 = time()
    output_data = masker(input_file.data, no_pd=True, no_output_json=True)
    output_filename = os.path.join(params.data.destination_folder,
                                   input_file.non_extension_part + '_processed.' + input_file.ext)
    input_file.save_data_to_file(output_data, params.data.destination_folder, params)
    message = f'File processing COMPLETED into: {output_filename} with time:{time() - f0}'
    click.echo(message)
    app_settings.logger.info(message)


def load_json_to_file_obj(file_object):
    try:
        # JSON file
        f = open(file_object.filename, "r", encoding='utf-8')
    except Exception:
        try:
            f = open(file_object.filename, "r", encoding='latin-1')
        except Exception:
            f = open(file_object.filename, "r", encoding='utf-8-sig')

    # Reading from file
    data = json.loads(f.read())

    # Checking the json structure
    if 'records' in data:
        file_object.data = DataFrame(data['records'])
    else:
        file_object.data = DataFrame(data)


def cli_file_read(filename):
    try:
        view_name = os.path.split(filename)[-1]
        obj = {
            "selected": True,
            "filename": filename,
            "ext": filename.split('.')[-1],
            "view_name": view_name,
            "is_cli": True
        }

        file_object = File(**obj)

        try:

            if file_object.ext == 'xlsx':
                file_object.data = read_excel(file_object.filename)

            if file_object.ext == 'csv':
                try:
                    file_object.data = read_csv(file_object.filename, encoding='utf-8')
                except Exception:
                    try:
                        file_object.data = read_csv(file_object.filename, encoding='latin-1')
                    except Exception:
                        file_object.data = read_csv(file_object.filename, encoding='utf-8-sig')

            if file_object.ext == 'json':
                load_json_to_file_obj(file_object)

            return file_object

        except Exception as e:
            click.echo(e.__str__())

    except Exception as e:
        message = f"Error parsing {filename}. {e}"
        click.echo(message)
        raise DipException(message)


def main(args):
    try:
        cli.main(args)
    except Exception as e:
        click.echo(e)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        cli.main(['--help'])
    else:
        cli()
