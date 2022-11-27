import click
import re
from urllib.parse import urlparse
from urllib.parse import parse_qs
import requests
import datetime
import os
import json
import gzip
from zipfile import ZipFile
from time import time
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError
import getpass
import pandas as pd

from cli_util import DipAuthException, DipException


class Extractor:
    def __init__(self, start_date, end_date, thread_id, app_settings=None,
                 filter_by_column=None, data_proccessor=None, mask_results=None,
                 all_dates=False, date_column=''):

        if (start_date == '-' or end_date == '-') and not all_dates:
            raise DipException('If --start_date or --end_date is not provides --all_date should be specified')

        if start_date == '-':
            start_date = datetime.datetime.now()
        if end_date == '-':
            end_date = datetime.datetime.now()
        self.settings = app_settings
        self.session = requests.Session()

        self.total_added = 0
        self.total_failed = 0
        self.offset = 0
        self.total_results = []
        self.response_size = 0
        self.maximum_trials_number = 2
        self.thread_params = None
        self.all_dates = all_dates
        self.date_column = date_column

        self.start_date = start_date
        self.end_date = end_date
        self.thread_id = thread_id
        self.filter_by_column = filter_by_column
        self.mask_results = mask_results
        self.data_proccessor = data_proccessor

    def api_extract(self, params):
        """
        :param url:
        :param start_date:
        :param end_date:
        :param batch_size:
        :return:
        """

        try:

            message = f"Thread {self.thread_id}: starting. Start_date: {self.start_date}"
            print(message)
            self.settings.logger.info(message)

            params.extracting.headers = {
                'Content-type': 'application/json'
            }

            if params.extracting.token:
                token = params.extracting.token
                params.extracting.headers['cookie'] = f'glide_user_activity={token}'

            self.__setup_auth(params)

            batch_start_date = self.start_date
            batch_end_date = batch_start_date + datetime.timedelta(hours=params.extracting.interval)
            formated_url = params.extracting.url.format(
                start_date=f'{batch_start_date}',
                end_date=f'{batch_end_date}',
                custom=''
            )
            use_user_url = formated_url != params.extracting.url
            while self.total_added < params.extracting.stop_limit and (batch_start_date < batch_end_date or self.all_dates):

                if batch_end_date >= self.end_date:
                    batch_end_date = self.end_date

                self.response_size = params.extracting.batch_size
                self.offset = 0

                self.__do_step(params, use_user_url, batch_start_date, batch_end_date)
                if self.all_dates:
                    #  If no dates specifiyed do only one step
                    break
                batch_start_date = batch_start_date + datetime.timedelta(hours=params.extracting.interval)
                batch_end_date = batch_end_date + datetime.timedelta(hours=params.extracting.interval)
        except DipAuthException as e:
            raise e
        except KeyboardInterrupt:
            message = f'Code Interrupted by User'
            print(message)
            self.settings.logger.error(message)
        except Exception as error:
            message = f"Error. Info: {error}"
            self.settings.logger.error(message)
            print(message)
        finally:

            # Save to file if any data is in buffer
            if len(self.total_results) > 0:
                self.save_data_to_file(self.total_results, params)

            message = f"Thread {self.thread_id}: finishing"
            print(message)
            self.settings.logger.info(message)

    def __do_step(self, params, use_user_url, batch_start_date, batch_end_date):

        while self.response_size >= params.extracting.batch_size and self.total_added < params.extracting.stop_limit:

            url = self.__get_request_url(use_user_url, params, batch_start_date, batch_end_date)
            trial_number = 1

            try:
                perv_offset = self.offset
                self.handle_api_request(params, url, trial_number)
                if self.offset == perv_offset:
                    # no new records
                    break

            except DipAuthException as e:
                raise e
            except ConnectionError:
                click.echo(click.style("Connection error", fg="red"))
                break
            except Exception:
                # Trials exceeded for this interval, jump to next interval
                break

    def __setup_auth(self, params):
        password = params.extracting.password
        if not params.extracting.token:
            if not password:
                password = getpass.getpass(prompt='Password: ', stream=None)

            params.extracting.auth = HTTPBasicAuth(params.extracting.username, password)

    def __get_request_url(self, use_user_url, params, batch_start_date, batch_end_date):
        parsed_url = urlparse(params.extracting.url)
        path = parsed_url.path
        date_column = 'sys_updated_on'
        if path.endswith('/sys_audit'):
            date_column = 'sys_created_on'
        if self.date_column:
            date_column = self.date_column
        # use user url, means that we are not constructing our sysparm_query
        # but using user provided url but we stil can replace some of its attributes such as
        # start_date and end_date
        if use_user_url:
            self.settings.logger.info('Going to use user url')
            url = params.extracting.url.format(
                start_date=f'{batch_start_date}',
                end_date=f'{batch_end_date}',
            )
            url += f'&sysparm_offset={self.offset}&sysparm_limit={params.extracting.batch_size}'
        else:
            url = params.extracting.url
            if 'sysparm_query=' in url:
                if not self.all_dates:
                    url += f'^{date_column}>{batch_start_date}^{date_column}<{batch_end_date}'
            else:
                if '?' not in url:
                    url += '?'
                else:
                    url += '&'
                if not self.all_dates:
                    url += f'sysparm_query={date_column}>{batch_start_date}^{date_column}<{batch_end_date}'
            if not url.endswith('&') and not url.endswith('?'):
                url += '&'
            url += f'sysparm_offset={self.offset}&sysparm_limit={params.extracting.batch_size}'

        message = f'Thread: {self.thread_id}, URL: {url}'
        print(message)
        self.settings.logger.info(message)

        return url

    def handle_api_request(self, params, url, trial_number):

        t1 = time()
        if params.extracting.token:
            resp = self.session.get(url, headers=params.extracting.headers)
        else:
            resp = self.session.get(url, headers=params.extracting.headers, auth=params.extracting.auth)
        if resp.status_code == 401:
            raise DipAuthException('Authentication failure')

        try:
            response_time = round(time() - t1, 3)

            self.offset += params.extracting.batch_size
            try:
                resp_body = json.loads(resp.text)
            except Exception as e:
                message = f"Failed to load response body using UTF-8. Going to use defults. {e}"
                self.settings.logger.error(message)
                click.echo(click.style(message, fg="yellow"))
                resp_body = resp.json()
            

            self.__process_response(resp, resp_body, params, response_time)

        except Exception as error:
            import traceback
            print(traceback.format_exc())
            if trial_number < self.maximum_trials_number:
                message = f"Error: Failed fetching from API. Trial: {trial_number} . Info: {error}"
                self.settings.logger.error(message)
                print(message)
                self.handle_api_request(params, url, trial_number + 1)

            if trial_number >= self.maximum_trials_number:
                message = f"Error: Totally Failed fetching from API. Trial: {trial_number} . Info: {error}"
                self.settings.logger.error(message)
                print(message)
                self.total_failed += params.extracting.batch_size
                raise DipException(message)

    def __process_response(self, resp, resp_body, params, response_time):
        if resp.status_code == 200 and resp_body.__contains__('result') and not resp_body.__contains__('error'):
            results = resp_body['result']

            # Validate results as json
            try:
                json.dumps(results)
            except Exception:
                raise DipException('Corrupted JSON from API')

            res_len = len(results)
            results = self.filter_by_column(results) if self.filter_by_column is not None else results
            if self.data_proccessor:
                self.data_proccessor(results)

            results = self.mask_results(results) if self.mask_results is not None else results

            filtered_len = res_len - len(results)
            self.settings.logger.info(f'Filtered out {filtered_len} of {res_len}')

            self.total_results += results
            self.response_size = len(results)
            self.total_added += self.response_size

            message = f'Added: {self.response_size}. (Total Added: {self.total_added}, \
Total Failed Approximated: {self.total_failed}), Response Time: {response_time} s'
            self.settings.logger.info(message)
            click.echo(click.style(message, fg="green", underline=True))

            if len(self.total_results) >= params.extracting.file_limit:
                message = 'File Split'
                print(message)
                self.settings.logger.info(message)
                self.save_data_to_file(self.total_results, params)
                self.total_results = []
        else:
            message = self.__get_err_message(resp)
            raise DipException(message)

    def __get_err_message(self, resp):
        if resp.json().__contains__('error'):
            message = f"{str(resp.json()['error'])}"
        else:
            message = f"{str(resp.json())}"
        return message

    def get_output_filename(self, params):
        prefix = 'output_'
        try:
            parsed_url = urlparse(params.extracting.url)
            prefix = parsed_url.path.rsplit('/', 1)[1]
            query = parsed_url.query
            tn = 'tablename'
            if tn in query:
                part = parsed_url.query[parsed_url.query.index(tn) + len(tn):]
                entity = re.findall(r'\w+', part)[0]
                prefix = f'{prefix}_{entity}_'
        except Exception:
            self.settings.logger.info(f'Failed parsing {params.extracting.url} using {prefix}')
        return os.path.join(params.extracting.output_dir,
                            prefix + self.settings.reset_timestamp() + '_' + str(self.thread_id))

    def save_data_to_file(self, results, params):
        try:
            print(self.thread_id)

            output_filename = self.get_output_filename(params)

            params.output_filename = output_filename

            # Validate results as json
            try:
                json.dumps(results[-1])

            except Exception:
                deleted = results[-1]
                results.pop()
                message = f'Deleted data to maintain JSON format: {deleted}'
                self.settings.logger.error(message)
                print(message)
            
            if params.extracting.output_format == 'json':
                self.save_json_file(results, params)
            if params.extracting.output_format == 'csv':
                self.save_csv_file(results, params)

            message = f'Writing to file COMPLETED SUCCESSFULLY for file:{output_filename}'
            self.settings.logger.info(message)
            print(message)

        except Exception as e:
            message = f'Error while saving file. {e}'
            self.settings.logger.exception(message)
            raise DipException(message)

    def save_json_file(self, results, params):
        try:
            self.write_to_json_file(results, params, 'utf-8')
        except Exception as e:
            print(e.__str__(), "\n", "Trying utf-8-sig encoding...")
            self.write_to_json_file(results, params, 'utf-8-sig')

    def save_csv_file(self, output_data, params):
        try:
            self.write_to_csv_file(output_data, params, 'utf-8')
        except Exception as e:
            print(e.__str__(), "\n", "Trying utf-8-sig encoding...")
            self.write_to_csv_file(output_data, params, 'utf-8-sig')

    def write_to_csv_file(self, results, params, encoding):
        output_filename = params.output_filename + '.csv'
        df = pd.DataFrame.from_dict(results)
        if params.extracting.compress:
            with gzip.open(output_filename + '.gz', 'wt', encoding=encoding) as file:
                df.to_csv(file, compression='gzip',index=False)
        else:
            with open(output_filename, 'w', encoding=encoding) as file:
                df.to_csv(file, index=False)

    def write_to_json_file(self, results, params, encoding):

        indent = None
        if params.extracting.pretty_json:
            indent = 4

        if params.extracting.compress and not params.masking.enabled:
            with gzip.open(params.output_filename + '.json.gz', 'wt', encoding=encoding) as zipfile:
                json.dump(results, zipfile, indent=indent, ensure_ascii=False)
        else:
            with open(params.output_filename + '.json', 'w', encoding=encoding) as f:
                json.dump(results, f, indent=indent, ensure_ascii=False)

            # ZipFile(f'{params.output_filename}.zip', mode='w').write(params.output_filename)

        # pass


class CsvFromJson:
    def __init__(self, settings, files_and_dirs, data_proccessor):
        self.settings = settings
        self.data_proccessor = data_proccessor
        self.files_and_dirs = files_and_dirs

    def create_csv(self):
        files_and_dirs = self.files_and_dirs
        for input_source in files_and_dirs:
            if os.path.isfile(input_source):
                self.proccess_file(input_source)
            elif os.path.isdir(input_source):
                self.proccess_dir(input_source)

    def proccess_file(self, file_path):
        if file_path.strip().endswith('.json'):
            try:
                with open(file_path) as f:
                    self.settings.logger.info(f'Going to proccess file: {file_path}')
                    data_list = json.load(f)
                    if not isinstance(data_list, list):
                        data_list = data_list["result"]
                    self.data_proccessor(data_list)
            except Exception as e:
                self.settings.logger.error(f"Error. Info: {e}")
        else:
            self.settings.logger.info(f"{file_path} doesn't have json extension, skipping it")

    def proccess_dir(self, dir_path):
        file_names = os.listdir(dir_path)
        for file_name in file_names:
            file_path = os.path.join(dir_path, file_name)
            self.proccess_file(file_path)


class DefaultDataProccessor:
    def __init__(self, out_path, prop_name):
        self.out_path = out_path
        self.prop_name = prop_name
        self.results_set = set()

    def __call__(self, items):
        if items is not None:
            prop_name = self.prop_name
            self.results_set.update([item[prop_name] for item in items if prop_name in item])

    def size(self):
        return len(self.results_set)

    def finalize(self):
        directory = os.path.dirname(self.out_path)
        if directory and directory != self.out_path and not os.path.isdir(directory):
            os.makedirs(directory)

        with open(self.out_path, 'w+') as out:
            out.write(f'{self.prop_name}\n')
            out.write('\n'.join(self.results_set))
