#!/usr/bin/python
import sys
import os
import getopt
import threading

from datetime import datetime, timedelta
from types import SimpleNamespace


def main(argv):
    try:
        help_message = '--url URL --output_dir OUTPUT_DIR --start_date START_DATE --end_date END_DATE ' \
                       '--username USERNAME --password PASSWORD --batch_size BATCH_SIZE --interval INTERVAL' \
                       ' --file_limit FILE_LIMIT [--stop_limit STOP_LIMIT] [--compress True] ' \
                       '[--parallel PARALLELISM_LEVEL]'


        params = params_initialize()

        message = f'Running DC_Exporter..'
        print(message)

        try:
            opts, args = getopt.getopt(argv, "hu:s:o:b:r:n:w:i:t:f:p:",
                                       ["h","url=", "start_date=", "end_date=", "output_dir=", "batch_size=",
                                        "compress=",
                                        "username=", "password=", "interval=","stop_limit=",
                                        "file_limit=", "parallel="])
        except getopt.GetoptError as e:
            print(e)
            print('Right usage: ',help_message)
            sys.exit(2)
        for opt, arg in opts:
            try:
                if opt == '-h':
                    print(help_message)
                    sys.exit()
                elif opt in ("--url"):
                    url = arg
                    params.url = arg
                elif opt in ("--start_date"):
                    params.start_date = datetime.fromisoformat(arg)
                elif opt in ("--end_date"):
                    params.end_date = datetime.fromisoformat(arg)
                elif opt in ("--batch_size"):
                    batch_size = int(arg)
                    params.batch_size = batch_size
                elif opt in ("--output_dir"):
                    params.output_dir = arg
                elif opt in ("--username"):
                    params.username = arg
                elif opt in ("--password"):
                    params.password = arg
                elif opt in ("--interval"):
                    params.interval = int(arg)
                elif opt in ("--compress"):
                    params.compress = bool(arg)
                    if params.compress:
                        params.extension = 'gz'
                elif opt in ("--stop_limit"):
                    params.stop_limit = int(arg)
                elif opt in ("--file_limit"):
                    params.file_limit = int(arg)
                elif opt in ("--parallel"):
                    params.parallelism_level = int(arg)

            except Exception as error:
                message = f'Error while parsing argument {opt}; {error}'
                print(message)
                sys.exit(2)




        cli_script_execute(params)

    except Exception as error:
        print(error)
        sys.exit(2)


def cli_script_execute(params):

    # Assert mandatory files/dirs
    try:
        for key in params.__dict__:
            assert params.__dict__[key] is not None, f'{key} argument is missing'

        if not params.__dict__.__contains__('output_dir'):
            dir = 'output'
            params.output_dir = os.path.join(os.getcwd(), dir)
            if not os.path.isdir(params.output_dir):
                os.mkdir(dir)

        message = f'Output directory is "{params.output_dir}'
        print(message)

        results = []
        pass

    except Exception as error:
        message = f'Execution Error: {error}'
        print(message)

    message = f'Script has FINISHED'
    print(message)

def params_initialize():

    params = SimpleNamespace()
    params.stop_limit = 1000000000
    params.file_limit = 1000000
    params.interval = 24
    params.batch_size = 1000
    params.parallelism_level = 1
    params.thread_id = 0
    params.extension = 'json'
    params.compress = False
    params.username = None
    params.password = None
    params.start_date = None
    params.end_date = None
    params.url = None

    return params


if __name__ == "__main__":
    main(sys.argv[1:])

