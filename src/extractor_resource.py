import requests
import datetime
import os
import json, gzip
from zipfile import ZipFile
from time import time
from requests.auth import HTTPBasicAuth


class Extractor:
    def __init__(self, start_date, end_date, thread_id, app_settings = None, filter_by_column=None):

        self.settings = app_settings
        self.session = requests.Session()

        self.total_added = 0
        self.total_failed = 0
        self.offset = 0
        self.total_results = []
        self.response_size = 0
        self.maximum_trials_number = 2
        self.thread_params = None
        self.start_date = None
        self.end_date = None
        self.thread_id = None

        self.start_date = start_date
        self.end_date = end_date
        self.thread_id = thread_id
        self.filter_by_column = filter_by_column


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
            params.extracting.auth = HTTPBasicAuth(params.extracting.username, params.extracting.password)

            batch_start_date = self.start_date
            batch_end_date = batch_start_date + datetime.timedelta(hours=params.extracting.interval)
            while self.total_added < params.extracting.stop_limit and batch_start_date < batch_end_date:

                if batch_end_date >= self.end_date:
                    batch_end_date = self.end_date

                self.response_size = params.extracting.batch_size
                self.offset = 0

                while self.response_size >= params.extracting.batch_size and self.total_added < params.extracting.stop_limit:
                    url = params.extracting.url
                    url += f'^sys_created_on>{batch_start_date}^sys_created_on<{batch_end_date}&sysparm_offset={self.offset}&sysparm_limit={params.extracting.batch_size}'
                    message = f'Thread: {self.thread_id}, URL: {url}'
                    print(message)
                    self.settings.logger.info(message)

                    trial_number = 1

                    try:
                        self.handle_api_request(params, url, trial_number)

                    except Exception as error:

                        # Trials exceeded for this interval, jump to next interval
                        break


                batch_start_date = batch_start_date + datetime.timedelta(hours=params.extracting.interval)
                batch_end_date = batch_end_date + datetime.timedelta(hours=params.extracting.interval)

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

    def handle_api_request(self, params, url, trial_number):
        try:
            t1 = time()
            resp = self.session.get(url, headers=params.extracting.headers, auth = params.extracting.auth)
            response_time = round(time() - t1, 3)

            self.offset += params.extracting.batch_size
            resp_body = resp.json()

            if resp.status_code == 200 and resp_body.__contains__('result') and not resp_body.__contains__('error'):
                results = resp_body['result']

                # Validate results as json
                try:
                    json.dumps(results)
                except Exception as error:
                    raise Exception('Corrupted JSON from API')
                
                res_len = len(results)
                results = self.filter_by_column(results) if self.filter_by_column is not None else results
                filtered_len = res_len - len(results)
                self.settings.logger.info(f'Filtered out {filtered_len} of {res_len}')

                self.total_results += results
                self.response_size = len(results)
                self.total_added += self.response_size

                message = f'Added: {self.response_size}. (Total Added: {self.total_added}, Total Failed Approximated: {self.total_failed}), Response Time: {response_time} s'
                self.settings.logger.info(message)
                print(message)

                if len(self.total_results) >= params.extracting.file_limit:
                    message = 'File Split'
                    print(message)
                    self.settings.logger.info(message)
                    self.save_data_to_file(self.total_results, params)
                    self.total_results = []
            else:
                message = ''
                if resp.json().__contains__('error'):
                    message = f"{str(resp.json()['error'])}"
                else:
                    message = f"{str(resp.json())}"

                raise Exception(message)

        except Exception as error:

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
                raise Exception(message)

    
    def get_output_filename(self, params):
        return os.path.join(params.extracting.output_dir,'output_' + self.settings.reset_timestamp() +
                            '_' + str(self.thread_id))

    def save_data_to_file(self, results, params):
        try:
            print(self.thread_id)

            output_filename = self.get_output_filename(params)

            params.output_filename = output_filename

            # Validate results as json
            try:
                json.dumps(results[-1])

            except Exception as error:
                deleted = results[-1]
                results.pop()
                message = f'Deleted data to maintain JSON format: {deleted}'
                self.settings.logger.error(message)
                print(message)

            try:
                self.write_to_json_file(results,params,'utf-8')
            except Exception as e:
                print(e.__str__(), "\n", "Trying utf-8-sig encoding...")

                self.write_to_json_file(results,params,'utf-8-sig')

            message = f'Writing to file COMPLETED SUCCESSFULLY for file:{output_filename}'
            self.settings.logger.info(message)
            print(message)

        except Exception as e:
            message = f'Error while saving file. {e}'
            self.settings.logger.exception(message)
            raise Exception(message)

    def write_to_json_file(self, results, params, encoding):


        if params.extracting.compress and not params.masking.enabled:
            with gzip.open(params.output_filename + '.json.gz', 'wt', encoding= encoding) as zipfile:
                json.dump(results, zipfile)
        else:
            with open(params.output_filename + '.json', 'w', encoding= encoding) as f:
                json.dump(results, f)

            # ZipFile(f'{params.output_filename}.zip', mode='w').write(params.output_filename)

        # pass



