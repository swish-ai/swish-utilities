import json
from unittest import TestCase
from pandas.io.parsers import read_csv
import requests_mock
from run import main
from src.extractor_resource import Extractor
from tests.common import SNOW_RESPONSE1, SNOW_RESPONSE_WITH_CUSTOM_ID, UNITEST_OUTPUT_FILE, UNITEST_OUTPUT_FILE_PREFIX, patch_for_tests
import os
import os.path
import shutil

OUTPUT_CSV_NAME = 'output.csv'
OUTPUT_CSV_DIR = 'processing_output'
OUTPUT_CSV_FILE = os.path.join(OUTPUT_CSV_DIR, OUTPUT_CSV_NAME)


class ProcessingTestCase(TestCase):

    def setUp(self) -> None:
        if os.path.isdir(OUTPUT_CSV_DIR):
            shutil.rmtree(OUTPUT_CSV_DIR)
    
    def test_proccessing(self):
        assert not os.path.isfile(OUTPUT_CSV_FILE)
        args = ["--proccess", "--out_props_csv_path", OUTPUT_CSV_FILE, "--out_prop_name", "sys_id"]
        main(args)
        assert os.path.isfile(OUTPUT_CSV_FILE)
    
    def test_proccessing_file(self):
        assert not os.path.isfile(OUTPUT_CSV_FILE)
        args = ["--proccess", "--input_sources", "tests/data/sys_audit.json", "--out_props_csv_path", OUTPUT_CSV_FILE]
        main(args)
        assert os.path.isfile(OUTPUT_CSV_FILE)
