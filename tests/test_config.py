import json
from unittest import TestCase, skip
from pandas.io.parsers import read_csv
import requests_mock
from run import cli
from cli_util import DipException
from tests.common import SNOW_RESPONSE1, SNOW_RESPONSE_WITH_CUSTOM_ID, UNITEST_OUTPUT_FILE, UNITEST_OUTPUT_FILE_PREFIX, patch_for_tests
import os
import os.path
from hashlib import sha256
from click.testing import CliRunner

patch_for_tests()


class FilteringTestCase(TestCase):

    def setUp(self) -> None:
        if os.path.isfile(UNITEST_OUTPUT_FILE):
            os.remove(UNITEST_OUTPUT_FILE)
        if os.path.isfile(f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json'):
            os.remove(f'{UNITEST_OUTPUT_FILE_PREFIX}_1.json')

    @skip("The auth functionallity is not use for now")
    def test_config_auth(self):
        assert not os.path.isfile(
            UNITEST_OUTPUT_FILE), "The file should be deleted"
        mock_session = requests_mock.Mocker()
        mock_session.register_uri(requests_mock.ANY,
                                  'https://dev71074.service-now.com/api/now/table/sys_audit',
                                  text=SNOW_RESPONSE1)
        mock_session.start()

        args = ["--extract", "--url", "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident",
                "--batch_size", "10000", "--file_limit", "50000",
                "--start_date", "2021-10-03", "--end_date", "2021-10-04"]
        runner = CliRunner()
        with self.assertRaises(Exception) as context:
            result = runner.invoke(cli, args, catch_exceptions=False)
            print(result.output)

        self.assertTrue(isinstance(context.exception, DipException))

        args.append('--auth_file')
        args.append('tests/data/fake_auth.json')

        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
    
    def test_mask_and_anonimyze_with_config(self):
        if os.path.isfile("tests/data/output/input_processed.json"):
            os.remove("tests/data/output/input_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output",
                "--output_format", "json",
                "--input_dir", "tests/data/input"]
        runner = CliRunner()
        with self.assertRaises(Exception) as context:
            result = runner.invoke(cli, args, catch_exceptions=False)
            print(result.output)
        print(context.exception)
        self.assertTrue(isinstance(context.exception, DipException))

        args.append('--config')
        args.append('tests/data/test_config.json')
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_processed.json")

        with open("tests/data/output/input_processed.json", 'r') as f:
            jsn = json.load(f)
            
            with open("tests/data/input/input.json", 'r') as f:
                jsn2 = json.load(f)
                for i, entry in enumerate(jsn2):
                    assert jsn[i]['sys_created_by'] == sha256(entry['sys_created_by'].encode('utf-8')).hexdigest()
                    # test conditional anonymization
                    if jsn2[i]['fieldname'] == 'state':
                        assert jsn[i]['record_checkpoint'] == sha256(entry['record_checkpoint'].encode('utf-8')).hexdigest()
                    else:
                        assert jsn[i]['record_checkpoint'] == entry['record_checkpoint']

                    # test conditional method
                    if jsn2[i]['fieldname'] == 'short_description':
                        assert jsn[i]['tablename'] == entry['tablename']
                    elif jsn2[i]['fieldname'] == 'state':
                        assert jsn[i]['tablename'] == sha256(entry['tablename'].encode('utf-8')).hexdigest()

    def test_patterns_with_config(self):
        if os.path.isfile("tests/data/output/input_pattern_processed.csv"):
            os.remove("tests/data/output/input_pattern_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_pattern",
                "--output_format", "csv",
                "--config", "tests/data/test_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_pattern_processed.csv")
        df = read_csv("tests/data/output/input_pattern_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))

        for entry in jsn:
            assert entry['documentkey'] == 'Very <#UNKNOWN> info about <#SOMETHING>'

    def test_types_with_config(self):
        if os.path.isfile("tests/data/output/input_boolean_processed.csv"):
            os.remove("tests/data/output/input_boolean_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_boolean",
                "--output_format", "csv",
                "--config", "tests/data/test_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_boolean_processed.csv")
        df = read_csv("tests/data/output/input_boolean_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))

        for entry in jsn:
            assert entry['test'] is True or entry['test'] is False
            assert entry['test2'] == 11.3
            assert str(entry['test3']) == '0' or str(entry['test3']) == '11'

    def test_default_config(self):
        if os.path.isfile("tests/data/output/input_default_processed.csv"):
            os.remove("tests/data/output/input_default_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_default",
                "--config", "default_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_default_processed.csv")
        df = read_csv("tests/data/output/input_default_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))

        for entry in jsn:
            assert entry['short_description'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['description'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['comments_and_work_notes'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['work_notes'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['close_notes'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['comments'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            if entry['fieldname'] == 'test':
                assert entry['oldvalue'] == 'test test@gmail.com 123456 +12 +123456789 192.168.11.2 www.google.com 778-62-8144'
                assert entry['newvalue'] == 'test test@gmail.com 123456 +12 +123456789 192.168.11.2 www.google.com 778-62-8144'
            else:
                assert entry['oldvalue'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
                assert entry['newvalue'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'

    def test_empty(self):
        if os.path.isfile("tests/data/output/no_json_processed.csv"):
            os.remove("tests/data/output/no_json_processed.csv")
        if os.path.isfile("tests/data/output/no_csv_processed.csv"):
            os.remove("tests/data/output/no_csv_processed.csv")
        if os.path.isfile("tests/data/output/white_space_processed.csv"):
            os.remove("tests/data/output/white_space_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/no_data_files",
                "--config", "default_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert not os.path.isfile("tests/data/output/no_json_processed.csv")
        assert not os.path.isfile("tests/data/output/no_csv_processed.csv")
        assert not os.path.isfile("tests/data/output/white_space_processed.csv")

    def test_inner_json(self):
        if os.path.isfile("tests/data/output/input_inner_json_processed.csv"):
            os.remove("tests/data/output/input_inner_json_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_inner_json",
                "--config", "default_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_inner_json_processed.csv")
        df = read_csv("tests/data/output/input_inner_json_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))

        for entry in jsn:
            assert entry['short_description'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['description'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['comments_and_work_notes'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['work_notes'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['close_notes'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            assert entry['comments'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
            if entry['fieldname'] == 'test':
                assert entry['oldvalue'] == 'test test@gmail.com 123456 +12 +123456789 192.168.11.2 www.google.com 778-62-8144'
                assert entry['newvalue'] == 'test test@gmail.com 123456 +12 +123456789 192.168.11.2 www.google.com 778-62-8144'
            else:
                assert entry['oldvalue'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'
                assert entry['newvalue'] == 'test  <#M>   <#>  +12 <#P>  <#I>   <#U>  <#P>'

    def test_preprocess_patterns(self):
        if os.path.isfile("tests/data/output/preprocess_patterns_processed.json"):
            os.remove("tests/data/output/preprocess_patterns_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/preprocess_patterns",
                "--output_format", "json",
                "--pattern", "[\w\-.]+\s+@[\w\-.]+:<#M>",
                "--config", "tests/data/preprocess_patterns_config.json",
                "--pretty_json", "--preprocess_patterns"]

        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/preprocess_patterns_processed.json")
        with open("tests/data/output/preprocess_patterns_processed.json", 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                assert '@' not in entry['short_description']

    def test_patterns_with_config_and_cli_input(self):
        if os.path.isfile("tests/data/output/input_pattern_processed.csv"):
            os.remove("tests/data/output/input_pattern_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_pattern",
                "--output_format", "csv",
                "--pattern", "info:qwerty",
                "--config", "tests/data/test_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_pattern_processed.csv")
        df = read_csv("tests/data/output/input_pattern_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))

        for entry in jsn:
            assert entry['documentkey'] == 'Very <#UNKNOWN> qwerty about <#SOMETHING>'

    def test_mask_single_file(self):
        if os.path.isfile("tests/data/output/input_a_processed.json"):
            os.remove("tests/data/output/input_a_processed.json")
        if os.path.isfile("tests/data/output/input_b_processed.json"):
            os.remove("tests/data/output/input_b_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_single/",
                "--input_file", "input_a.json",
                "--output_format", "json",
                "--pattern", "\\b(\\d+[a-zA-Z]|[a-zA-Z]+\\d)[\\w\\-\\_\\!\\?\\.\\#\\$\\%\\^\\&\\*\\.\\(\\)\\\\\\/]+\\b:<#CG>",
                "--config", "tests/data/test_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_a_processed.json")
        assert not os.path.isfile("tests/data/output/input_b_processed.json")

        with open("tests/data/output/input_a_processed.json", 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                assert entry['documentkey'] == '<#CG>'

    def test_filter_and_mask(self):
        if os.path.isfile("tests/data/output/input_processed.json"):
            os.remove("tests/data/output/input_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input/", "--output_format", "json",
                "--config", "tests/data/test_filter_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_processed.json")

        with open("tests/data/output/input_processed.json", 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                assert entry["fieldname"] in ["incident_state", "comments", "closed_at"]
    
    def test_nested_filter_and_mask(self):
        if os.path.isfile("tests/data/output/inner/input_csv_processed.csv"):
            os.remove("tests/data/output/inner/input_csv_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input_nested/",
                "--config", "tests/data/test_filter_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/inner/input_csv_processed.csv")

        df = read_csv("tests/data/output/inner/input_csv_processed.csv", encoding="UTF-8")
        jsn = json.loads(df.to_json(orient='records'))
        for entry in jsn:
            assert entry["fieldname"] in ["incident_state", "comments", "closed_at"]

    def test_white_list_mask(self):
        if os.path.isfile("tests/data/output/input_processed.json"):
            os.remove("tests/data/output/input_processed.json")

        args = ["--mask", "--output_dir", "tests/data/output", 
                "--input_dir", "tests/data/input/", "--output_format", "json",
                "--config", "tests/data/test_white_list_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output/input_processed.json")

        with open("tests/data/output/input_processed.json", 'r') as f:
            jsn = json.load(f)
            for entry in jsn:
                for key in entry:
                    assert key in ["fieldname", "reason", "sys_id"]
    
    def test_ip_and_ttt_in_end_of_word_mask(self):
        if os.path.isfile("tests/data/output_ip_ttt/ip_ttt_processed.csv"):
            os.remove("tests/data/output_ip_ttt/ip_ttt_processed.csv")

        args = ["--mask", "--output_dir", "tests/data/output_ip_ttt", 
                "--input_dir", "tests/data/input_ip_ttt",
                "--config", "tests/data/ip_ttt_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output_ip_ttt/ip_ttt_processed.csv")
    
    def test_file_with_multiple_extensions_mask(self):
        if os.path.isfile("tests/data/output_multi_extensions/test_multi_processed.multi.csv"):
            os.remove("tests/data/output_multi_extensions/test_multi_processed.multi.csv")
        if os.path.isfile("tests/data/output_multi_extensions/test_multi_2_processed.multi.csv"):
            os.remove("tests/data/output_multi_extensions/test_multi_2_processed.multi.csv")

        args = ["--mask", "--output_dir", "tests/data/output_multi_extensions", 
                "--input_dir", "tests/data/input_multi_extensions",
                "--config", "default_config.json"]
        runner = CliRunner()
        result = runner.invoke(cli, args, catch_exceptions=False)
        print(result.output)
        assert result.exit_code == 0
        assert os.path.isfile("tests/data/output_multi_extensions/test_multi_processed.multi.csv")
        assert os.path.isfile("tests/data/output_multi_extensions/test_multi_2_processed.multi.csv")