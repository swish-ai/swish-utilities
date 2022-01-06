import json
from unittest import TestCase
from pandas.io.parsers import read_csv
import requests_mock
from run import cli
from tests.common import SNOW_RESPONSE1, SNOW_RESPONSE_WITH_CUSTOM_ID, UNITEST_OUTPUT_FILE, UNITEST_OUTPUT_FILE_PREFIX, patch_for_tests
import os
import os.path
from click.testing import CliRunner
import csv

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
        
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        assert os.path.isfile(UNITEST_OUTPUT_FILE) and os.path.isfile(f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json')
        

    def test_mask(self):

        if os.path.isfile("tests/data/output/input_processed.json"):
            os.remove("tests/data/output/input_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input",
                "--mapping_path", "tests/data/mapping_file.csv", 
                "--custom_token_dir", "tests/data/custom", 
                "--important_token_file", "tests/data/important_tokens.txt"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        assert os.path.isfile("tests/data/output/input_processed.json")

        with open("tests/data/output/input_processed.json", 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                assert entry['documentkey'] == '<#CG>'
                assert 'record_checkpoint' not in entry
    
    def test_csv_mask(self):

        if os.path.isfile("tests/data/output/input_csv_processed.csv"):
            os.remove("tests/data/output/input_csv_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--output_format", "csv",
                "--input_dir", "tests/data/input_csv",
                "--mapping_path", "tests/data/mapping_file.csv", 
                "--custom_token_dir", "tests/data/custom", 
                "--important_token_file", "tests/data/important_tokens.txt"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        assert os.path.isfile("tests/data/output/input_csv_processed.csv")
        df = read_csv("tests/data/output/input_csv_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))
        assert len(jsn) == 84
        for entry in jsn:
            assert entry['documentkey'] == '<#CG>'
            assert 'record_checkpoint' not in entry

    def test_csv_mask_with_cunksize(self):

        if os.path.isfile("tests/data/output/input_csv_processed.csv"):
            os.remove("tests/data/output/input_csv_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--output_format", "csv", "--csv_chunk_size", "2",
                "--input_dir", "tests/data/input_csv",
                "--mapping_path", "tests/data/mapping_file.csv", 
                "--custom_token_dir", "tests/data/custom", 
                "--important_token_file", "tests/data/important_tokens.txt"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        assert os.path.isfile("tests/data/output/input_csv_processed.csv")
        df = read_csv("tests/data/output/input_csv_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))
        assert len(jsn) == 84
        for entry in jsn:
            assert entry['documentkey'] == '<#CG>'
            assert 'record_checkpoint' not in entry

    def test_csv_mask_with_cunksize_and_json_output(self):

        if os.path.isfile("tests/data/output/input_csv_processed.json"):
            os.remove("tests/data/output/input_csv_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--output_format", "json", "--csv_chunk_size", "2",
                "--input_dir", "tests/data/input_csv",
                "--mapping_path", "tests/data/mapping_file.csv", 
                "--custom_token_dir", "tests/data/custom", 
                "--important_token_file", "tests/data/important_tokens.txt"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        assert os.path.isfile("tests/data/output/input_csv_processed.json")
        with open("tests/data/output/input_csv_processed.json", 'r') as f:
            jsn = json.load(f)
            assert len(jsn) == 84
            for entry in jsn:
                assert entry['documentkey'] == '<#CG>'
                assert 'record_checkpoint' not in entry
    
    def test_mask_to_csv(self):

        if os.path.isfile("tests/data/output/input_processed.csv"):
            os.remove("tests/data/output/input_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input",
                "--mapping_path", "tests/data/mapping_file.csv", 
                "--custom_token_dir", "tests/data/custom", 
                "--important_token_file", "tests/data/important_tokens.txt",
                "--output_format", "csv"
                ]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        assert os.path.isfile("tests/data/output/input_processed.csv")

        with open("tests/data/output/input_processed.csv", 'r') as f:
            content = csv.DictReader(f)
            for entry in content:
                assert entry['documentkey'] == '<#CG>'


    def test_real_time_test_mask(self):
        with open('tests/data/sys_audit.json', 'r') as f:
            mock_data = f.read()

        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        
        

        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=mock_data)
        mock_session.start()

        args = ["--extract", "--export_and_mask", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--username", "fake_user", "--password", "fake_pass",  "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-07-16", "--end_date", "2021-07-16", "--mapping_path", "tests/data/mapping_file.csv",
                "--custom_token_dir", "tests/data/custom"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        assert result.exit_code == 0


        assert os.path.isfile(UNITEST_OUTPUT_FILE)
        with open(UNITEST_OUTPUT_FILE, 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                assert entry['documentkey'] == '<#CG>'


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
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0


        assert os.path.isfile(UNITEST_OUTPUT_FILE)

    def test_exctarct_csv(self):

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
                "--start_date", "2021-07-16", "--end_date", "2021-07-16", "--out_props_csv_path", "dock_ids.csv"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0

        data = read_csv(f"dock_ids.csv", encoding='utf-8')
        assert len(data['documentkey'].values) == 9, "wrong output count"

        assert os.path.isfile(UNITEST_OUTPUT_FILE)
        
