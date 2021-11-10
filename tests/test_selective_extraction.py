import json
from unittest import TestCase
from pandas.io.parsers import read_csv
import requests_mock
from run import main
from tests.common import SNOW_RESPONSE1, SNOW_RESPONSE_WITH_CUSTOM_ID, UNITEST_OUTPUT_FILE, UNITEST_OUTPUT_FILE_PREFIX, patch_for_tests
import os
import os.path

patch_for_tests()

TEST_IDS_FILE = "tests/data/sys_ids_list.csv"
TEST_IDS_FILE2 = "tests/data/custom_ids_list.csv"


class SelectiveExtractionTesting(TestCase):

    def setUp(self) -> None:
        if os.path.isfile(UNITEST_OUTPUT_FILE):
            os.remove(UNITEST_OUTPUT_FILE)

    def test_no_filter(self):
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=SNOW_RESPONSE1)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)

        assert len([d for d in data if d['sys_id'] ==
                   '669009b4874330105fd965f73cbb3533'])
        assert len([d for d in data if d['sys_id'] == '123'])
        assert len([d for d in data if d['sys_id'] == '456'])
        assert not len([d for d in data if d['sys_id'] == '567'])

    def test_selective(self):
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=SNOW_RESPONSE1)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE]
        main(args)

        assert os.path.isfile(
            UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)

        assert len([d for d in data if d['sys_id'] ==
                   '669009b4874330105fd965f73cbb3533']) == 0

        ids = read_csv(TEST_IDS_FILE, encoding='utf-8')['sys_id']
        for id in ids:
            assert len([d for d in data if d['sys_id'] == str(id)])
    
    def test_selective_with_prop_extraction(self):
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=SNOW_RESPONSE1)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE,
                "--out_props_csv_path", "qwerty.csv", "--out_prop_name", "sys_created_on"]
        main(args)

        data = read_csv(f"qwerty.csv", encoding='utf-8')
        assert len(data['sys_created_on'].values) == 1, "wrong output count"

    def test_selective_with_id_column(self):
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=SNOW_RESPONSE_WITH_CUSTOM_ID)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE2,
                "--id_field_name", "custom_sys_id"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)

        assert len([d for d in data if d['custom_sys_id'] ==
                   '669009b4874330105fd965f73cbb3533']) == 0

        ids = read_csv(TEST_IDS_FILE2, encoding='utf-8')['custom_sys_id']
        for id in ids:
            assert len([d for d in data if d['custom_sys_id'] == str(id)])

    def test_selective_with_data_id_name(self):
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=SNOW_RESPONSE_WITH_CUSTOM_ID)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", TEST_IDS_FILE2,
                "--id_field_name", "custom_sys_id", "--data_id_name", "custom_sys_id"]
        main(args)

        assert os.path.isfile(UNITEST_OUTPUT_FILE), "No output file found"

        data = []
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            data = json.load(f)

        assert len([d for d in data if d['custom_sys_id'] ==
                   '669009b4874330105fd965f73cbb3533']) == 0

        ids = read_csv(TEST_IDS_FILE, encoding='utf-8')['sys_id']
        for id in ids:
            assert len([d for d in data if d['custom_sys_id'] == str(id)])

    def test_file_doesnt_exist(self):
        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04", "--id_list_path", "qwqweqweqweqew"]
        main(args)
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
