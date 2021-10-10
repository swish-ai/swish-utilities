#!/usr/bin/python
import sys
import os
import json
import getopt
import threading

from datetime import datetime, timedelta
from src.data_filter import ColumnFilter
from src.extractor_resource import Extractor
from types import SimpleNamespace
from src.settings import Settings
from src.file import File
from pandas import read_csv, read_excel, DataFrame
from src.cleaner import TextCleaner, CustomUserFile
from time import time


# ENUMS
MASK: int = 1
ANONYMIZE: int = 2
NONE_ACTION: int = 3




def main(argv):
    try:
        help_message = '--url URL [--mask] [--extract] --output_dir OUTPUT_DIR --start_date START_DATE --end_date END_DATE ' \
                       '--username USERNAME --password PASSWORD --batch_size BATCH_SIZE --interval INTERVAL' \
                       ' --file_limit FILE_LIMIT [--stop_limit STOP_LIMIT] [--compress] ' \
                       '[--parallel PARALLELISM_LEVEL]' \
                       '--input_dir INPUT_DIR --mapping_path MAPPING_PATH ' \
                       '--custom_token_dir CUSTOM_TOKEN_DIR [--important_token_path IMPORTANT_TOKEN_PATH] ' \
                       '[--id_list_path path to filtering file in csv format] '\
                       '[--id_field_name name of field in the filtering file] ' \
                       '[--data_id_name name of field in the source data] '

        app_settings = Settings('base')
        params = params_initialize()

        message = f'Running DC_Utilities..'
        app_settings.logger.info(message)
        print(message)

        try:
            opts, args = getopt.getopt(argv, "hm:e:u:s:o:b:r:n:w:i:t:f:p:a:c:d:g",
                                       ["h",
                                        "mask", "extract",
                                        "url=",
                                        "start_date=", "end_date=", "output_dir=", "batch_size=",
                                        "compress",
                                        "username=", "password=", "interval=","stop_limit=",
                                        "file_limit=", "parallel=",
                                        "input_dir=", "mapping_path=", "custom_token_dir=",
                                        "important_token_file=",
                                        "id_list_path=", "id_field_name=", "data_id_name="])
        except getopt.GetoptError as e:
            print(e)
            print('Right usage: ',help_message)
            sys.exit(2)
        for opt, arg in opts:
            try:
                if opt == '-h':
                    print(help_message)
                    sys.exit()
                elif opt in ("--mask"):
                    params.masking.enabled = True
                elif opt in ("--extract"):
                    params.extracting.enabled = True
                elif opt in ("--url"):
                    url = arg
                    params.extracting.url = arg
                elif opt in ("--start_date"):
                    params.extracting.start_date = datetime.fromisoformat(arg)
                elif opt in ("--end_date"):
                    params.extracting.end_date = datetime.fromisoformat(arg)
                elif opt in ("--batch_size"):
                    batch_size = int(arg)
                    params.extracting.batch_size = batch_size
                elif opt in ("--output_dir"):
                    params.extracting.output_dir = arg
                    params.masking.output_dir = arg
                elif opt in ("--username"):
                    params.extracting.username = arg
                elif opt in ("--password"):
                    params.extracting.password = arg
                elif opt in ("--interval"):
                    params.extracting.interval = float(arg)
                elif opt in ("--compress"):
                    params.extracting.compress = True
                    params.masking.compress = True
                    params.extracting.extension = 'gz'
                elif opt in ("--stop_limit"):
                    params.extracting.stop_limit = int(arg)
                elif opt in ("--file_limit"):
                    params.extracting.file_limit = int(arg)
                elif opt in ("--parallel"):
                    params.extracting.parallelism_level = int(arg)
                elif opt in ("--id_list_path"):
                    params.extracting.id_list_path = arg
                elif opt in ("--id_field_name"):
                    params.extracting.id_field_name = arg
                elif opt in ("--data_id_name"):
                    params.extracting.data_id_name = arg

                # Masking Arguments
                elif opt in ("--input_dir"):
                    params.masking.input_dir = arg
                elif opt in ("--mapping_path"):
                    params.masking.mapping_path = arg
                elif opt in ("--custom_token_dir"):
                    params.masking.custom_token_dir = arg
                elif opt in ("--important_token_file"):
                    params.masking.important_token_file = arg

            except Exception as error:
                message = f'Error while parsing argument {opt}; {error}'
                print(message)
                sys.exit(2)




        cli_script_execute(params, app_settings)

    except Exception as error:
        app_settings.logger.exception(error)
        print(error)
        sys.exit(2)


def cli_script_execute(params, app_settings):

    # Assert mandatory files/dirs
    try:


        validate_params(params.extracting, 'extracting')

        validate_params(params.masking, 'masking')

        if params.extracting.enabled and params.masking.enabled:
            params.masking.input_dir = params.extracting.output_dir

        try:
            if params.extracting.enabled:
                filter_by_column = None
                if params.extracting.id_list_path:
                    ids_file = cli_file_read(params.extracting.id_list_path)
                    filter_by_column = ColumnFilter(ids_file.data, params.extracting.id_field_name, params.extracting.data_id_name)
                extracting_execute(params, app_settings, filter_by_column)
        except Exception as error:
            raise Exception(f'Extracting error, {error}')

        try:
            if params.masking.enabled:
                masking_execute(params.masking, app_settings)
        except Exception as error:
            raise Exception(f'Masking error, {error}')


    except Exception as error:
        message = f'Execution Error: {error}'
        print(message)
        app_settings.logger.exception(message)

    message = f'Script has FINISHED'
    print(message)


def validate_params(params, type):
    """

    :param params:
    :param type:
    :return:
    """

    if params.enabled:
        for key in params.__dict__:
            assert params.__dict__[key] is not None, f'{key} argument is missing'

        if not params.__dict__.__contains__('output_dir'):
            dir = f'{type}_output'
            params.output_dir = os.path.join(os.getcwd(), dir)
            if not os.path.isdir(params.output_dir):
                os.mkdir(dir)

        message = f'{type} output directory is "{params.output_dir}'
        print(message)

def extracting_execute(params, app_settings, filter_by_column):

    # Assert mandatory files/dirs
    try:
        for key in params.extracting.__dict__:
            assert params.extracting.__dict__[key] is not None, f'{key} argument is missing'

        if not params.extracting.__dict__.__contains__('output_dir'):
            dir = 'output'
            params.extracting.output_dir = os.path.join(os.getcwd(), dir)
            if not os.path.isdir(params.extracting.output_dir):
                os.mkdir(dir)

        message = f'Output directory is "{params.extracting.output_dir}'
        app_settings.logger.info(message)
        print(message)

        results = []
        if params.extracting.parallelism_level > 1 :
            extracting_multithreading_execution(params, app_settings, filter_by_column)
        else:
            api_resource= Extractor(params.extracting.start_date, params.extracting.end_date, 0, app_settings, filter_by_column=filter_by_column)
            api_resource.api_extract(params)

            message = f'Total Added: {api_resource.total_added}, Total Failed Approximated: {api_resource.total_failed}'
            app_settings.logger.info(message)
            print(message)

    except Exception as error:
        message = f'Execution Error: {error}'
        print(message)
        app_settings.logger.error(message)

    message = f'Extracting has FINISHED'
    app_settings.logger.info(message)
    print(message)

def extracting_multithreading_execution(params, app_settings, filter_by_column):

    thread_list = list()
    total_period = params.extracting.end_date - params.extracting.start_date
    single_period = total_period.total_seconds() / params.extracting.parallelism_level
    batch_start_date = params.extracting.start_date
    batch_end_date = batch_start_date + timedelta(seconds=single_period)

    resources = [None] * params.extracting.parallelism_level
    for thread_id in range(params.extracting.parallelism_level):
        thread_params  = params
        thread_params.extracting.start_date = batch_start_date
        thread_params.extracting.end_date = batch_end_date
        thread_params.extracting.thread_id = thread_id

        resources[thread_id] = Extractor(batch_start_date, batch_end_date, thread_id, Settings(str(thread_id)), filter_by_column=filter_by_column)

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
        print("Main    : before joining thread ", index)
        thread.join()
        print("Main    : thread ", index, " done")
        total_added += resources[index].total_added
        total_failed += resources[index].total_failed

    message = f'Total Added: {total_added}, Total Failed Approximated: {total_failed}'
    app_settings.logger.info(message)
    print(message)



def masking_execute(params, app_settings):

    message = f'Masking Started..'
    print(message)
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
        custom_tokens_filename_list = [f for f in os.listdir(params.custom_token_dir) if os.path.isfile(os.path.join(params.custom_token_dir, f))]
        for f in custom_tokens_filename_list:
            params.data.custom_tokens_filename_list.append(os.path.join(params.custom_token_dir, f))

    params.data.cleaner = TextCleaner(params.data.custom_tokens_filename_list, important_token_file)

    # Read mandatory files
    mapping_file = cli_file_read(params.mapping_path)
    params.data.mapping_file_objects.append(mapping_file)
    params.data.destination_folder = params.output_dir

    # Read input files and execute
    input_files = [f for f in os.listdir(params.input_dir) if os.path.isfile(os.path.join(params.input_dir, f))]
    for f in input_files:

        # if f.find('sys_choice') == -1 and f.find('cmn_schedule_span') == -1 and f.find('sys_user_grmember') == -1:
            input_file = cli_file_read(os.path.join(params.input_dir, f))
            params.data.file_objects.append(input_file)
            cli_file_process(input_file, mapping_file, params.data.cleaner, params, app_settings)
        # else:
        #     input_file = cli_file_read(os.path.join(params.input_dir, f))
        #     input_file.save_data_to_file(input_file.data, params.data.destination_folder)


def cli_file_process(input_file, mapping_file, cleaner, params, app_settings):
    try:
        message = f"Processing input file: {input_file.filename}"
        print(message)
        app_settings.logger.info(message)
        f0 = time()
        output_data = input_file.data

        for column in output_data:
            message = f'Column: {column} | Start", end=" | '
            print(message)
            app_settings.logger.info(message)

            c0 = time()
            method = NONE_ACTION

            if mapping_file.filename != '' and \
                    mapping_file.data is not None and \
                    column in mapping_file.data['column'].to_list() and \
                    mapping_file.data[mapping_file.data['column'] == column]['method'].item() is not None:

                method = mapping_file.data[mapping_file.data['column'] == column]['method'].item()

            if method == NONE_ACTION:
                pass
            elif method == MASK:
                pass
            elif method == ANONYMIZE:

                if params.data.custom_tokens_filename_list is not None and params.data.custom_tokens_filename_list != []:

                    output_data[column] = output_data[column].fillna('')
                    output_data[column] = cleaner.transform(output_data[column].values.tolist())

            message = f'End | took: {time()-c0}'
            print(message)
            app_settings.logger.info(message)

        output_filename = os.path.join(params.data.destination_folder,
                                       input_file.non_extension_part + '_processed.' + input_file.ext)
        input_file.save_data_to_file(output_data, params.data.destination_folder, params)

        message = f'File processing COMPLETED into: {output_filename} with time:{time() - f0}'
        print(message)
        app_settings.logger.info(message)

    except Exception as e:
        message = f'File processing FAILED for file: {input_file.filename}. Reason: {e}'
        print(message)
        app_settings.logger.info(message)


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
                try:
                    # JSON file
                    f = open(file_object.filename, "r",encoding='utf-8')
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

            return file_object

        except Exception as e:
            print(e.__str__())

    except Exception as e:
        message = f"Error parsing {filename}. {e}"
        print(message)
        raise Exception(message)

def params_initialize():

    params = SimpleNamespace()
    params.masking = SimpleNamespace()
    params.extracting = SimpleNamespace()

    params.masking.enabled = False
    params.extracting.enabled = False

    params.extracting.stop_limit = 1000000000
    params.extracting.file_limit = 1000000
    params.extracting.interval = 24
    params.extracting.batch_size = 1000
    params.extracting.parallelism_level = 1
    params.extracting.thread_id = 0
    params.extracting.extension = 'json'
    params.extracting.compress = False
    params.extracting.username = None
    params.extracting.password = None
    params.extracting.start_date = None
    params.extracting.end_date = None
    params.extracting.url = None
    params.extracting.id_list_path = ''
    params.extracting.id_field_name = 'sys_id'
    params.extracting.data_id_name = ''


    # params.masking.input_dir = None
    params.masking.mapping_path = None
    params.masking.custom_token_dir = ''
    params.masking.important_token_file = None
    params.masking.compress = False

    params.masking.data = SimpleNamespace()
    params.masking.data.user_selected_file_list = {}
    params.masking.data.custom_tokens_filename = None
    params.masking.data.custom_tokens_filename_list = []
    params.masking.data.destination_folder = None
    params.masking.data.file_objects = []
    params.masking.data.mapping_file_objects = []
    params.masking.data.file_counter = 0
    params.masking.data.chunk_size = 50

    return params


if __name__ == "__main__":
    main(sys.argv[1:])

