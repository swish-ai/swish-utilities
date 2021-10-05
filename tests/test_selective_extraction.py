import json
from unittest import TestCase
from pandas.io.parsers import read_csv
import requests_mock
from run import main
from src.extractor_resource import Extractor
from tests.common import SNOW_RESPONSE1, SNOW_RESPONSE_WITH_CUSTOM_ID
import os
import os.path


TEST_IDS_FILE = "tests/data/sys_ids_list.csv"
TEST_IDS_FILE2 = "tests/data/custom_ids_list.csv"

# monkeypatch Extractor for tests
UNITEST_OUTPUT_FILE_PREFIX = 'extracting_output/__unitest_output'
UNITEST_OUTPUT_FILE = f'{UNITEST_OUTPUT_FILE_PREFIX}.json'
Extractor.get_output_filename = lambda self, params: UNITEST_OUTPUT_FILE_PREFIX


class SelectiveExtractionTesting(TestCase):

    def setUp(self) -> None:
        if os.path.isfile(UNITEST_OUTPUT_FILE):
            os.remove(UNITEST_OUTPUT_FILE)
    
    def test_no_filter(self):
        assert not os.path.isfile(UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table',
                                  text=SNOW_RESPONSE1)
        mock_session.start()
        
        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000", 
                "--start_date", "2021-10-03", "--end_date", "2021-10-04"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)
        
        assert len([d for d in data if d['sys_id'] == '669009b4874330105fd965f73cbb3533'])
        assert len([d for d in data if d['sys_id'] == '123'])
        assert len([d for d in data if d['sys_id'] == '456'])
        assert not len([d for d in data if d['sys_id'] == '567'])


    def test_selective(self):
        assert not os.path.isfile(UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table',
                                  text=SNOW_RESPONSE1)
        mock_session.start()
        
        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000", 
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)
        
        assert len([d for d in data if d['sys_id'] == '669009b4874330105fd965f73cbb3533']) == 0

        ids = read_csv(TEST_IDS_FILE, encoding='utf-8')['sys_id']
        for id in ids:
            assert len([d for d in data if d['sys_id'] == str(id)])
    
    def test_selective_with_id_column(self):
        assert not os.path.isfile(UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table',
                                  text=SNOW_RESPONSE1)
        mock_session.start()
        
        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000", 
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE2, 
                "--id_field_name", "custom_sys_id"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)
        
        assert len([d for d in data if d['sys_id'] == '669009b4874330105fd965f73cbb3533']) == 0

        ids = read_csv(TEST_IDS_FILE, encoding='utf-8')['sys_id']
        for id in ids:
            assert len([d for d in data if d['sys_id'] == str(id)])
    
    def test_selective_with_data_id_name(self):
        assert not os.path.isfile(UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table',
                                  text=SNOW_RESPONSE_WITH_CUSTOM_ID)
        mock_session.start()
        
        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000", 
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE2, 
                "--id_field_name", "custom_sys_id", "--data_id_name", "custom_sys_id"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)
        
        assert len([d for d in data if d['custom_sys_id'] == '669009b4874330105fd965f73cbb3533']) == 0

        ids = read_csv(TEST_IDS_FILE, encoding='utf-8')['sys_id']
        for id in ids:
            assert len([d for d in data if d['custom_sys_id'] == str(id)])

    def test_file_doesnt_exist(self):
        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/x_707785_someapp_test_table",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000", 
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", "qwqweqweqweqew"]
        main(args)
        assert not os.path.isfile(UNITEST_OUTPUT_FILE), "The file should be deleted"
