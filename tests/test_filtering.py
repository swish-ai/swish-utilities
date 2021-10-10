import json
from unittest import TestCase
from pandas.io.parsers import read_csv
import requests_mock
from run import main
from src.extractor_resource import Extractor
from tests.common import SNOW_RESPONSE1, SNOW_RESPONSE_WITH_CUSTOM_ID, UNITEST_OUTPUT_FILE, UNITEST_OUTPUT_FILE_PREFIX, patch_for_tests
import os
import os.path

patch_for_tests()


class FilteringTestCase(TestCase):

    def setUp(self) -> None:
        if os.path.isfile(UNITEST_OUTPUT_FILE):
            os.remove(UNITEST_OUTPUT_FILE)
        if os.path.isfile(f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json'):
            os.remove(f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json')

    def test_parallel(self):

        with open('tests/data/sys_audit.json', 'r') as f:
            mock_data = f.read()

        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        
        assert not os.path.isfile(
            f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json'), "The file should be deleted"

        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=mock_data)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-07-16", "--end_date", "2021-07-16", "--id_list_path", "tests/data/user_types.csv",
            "--id_field_name", "user", "--parallel", "2"]
        
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE) and os.path.isfile(f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json')
        

    def test_mask(self):

        if os.path.isfile("tests/data/output/input_processed.json"):
            os.remove("tests/data/output/input_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input",
                "--mapping_path", "tests/data/mapping_file.csv", 
                "--custom_token_dir", "tests/data/custom", 
                "--important_token_file", "tests/data/important_tokens.txt"]
        main(args)

        assert os.path.isfile("tests/data/output/input_processed.json")

        with open("tests/data/output/input_processed.json", 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                assert entry['sys_id'] == '<#CG>'

    def test_filtering_wiht_user_admin(self):

        with open('tests/data/sys_audit.json', 'r') as f:
            mock_data = f.read()

        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        
        

        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=mock_data)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-07-16", "--end_date", "2021-07-16", "--id_list_path", "tests/data/user_types.csv",
                "--id_field_name", "user"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE)
        
