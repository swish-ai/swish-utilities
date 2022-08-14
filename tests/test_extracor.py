from datetime import datetime
from unittest import TestCase
from types import SimpleNamespace
from urllib import parse
from src.extractor_resource import CsvFromJson, DefaultDataProccessor, Extractor


class TestExtractor(TestCase):
    def test_filter_dates_column_sys_audit(self):
        foo = lambda msg: None
        start_date = datetime.strptime("2021-10-03", "%Y-%m-%d").date()
        end_date = datetime.strptime("2021-10-04", "%Y-%m-%d").date()
        thread_id = 0
        app_settings = SimpleNamespace(
            logger=SimpleNamespace(info=foo)
        )
        filter_by_column = None
        data_proccessor = None
        mask_results = None,
        all_dates = False
        date_column = ''

        extractor = Extractor(start_date, end_date, thread_id, app_settings,
                              filter_by_column, data_proccessor, mask_results,
                              all_dates, date_column)
        url = "https://dev71074.service-now.com/api/now/table/sys_audit?sysparm_query=tablename=incident"
        params = SimpleNamespace(
            extracting=SimpleNamespace(url=url, batch_size=1000),
        )
        url = extractor._Extractor__get_request_url(False, params, start_date, end_date)
        parsed = parse.parse_qs(parse.urlsplit(url).query)
        assert 'sys_updated_on' not in str(parsed['sysparm_query'])
        assert 'sys_created_on' in str(parsed['sysparm_query'])

    def test_filter_dates_column_custom_date_column(self):
        foo = lambda msg: None
        start_date = datetime.strptime("2021-10-03", "%Y-%m-%d").date()
        end_date = datetime.strptime("2021-10-04", "%Y-%m-%d").date()
        thread_id = 0
        app_settings = SimpleNamespace(
            logger=SimpleNamespace(info=foo)
        )
        filter_by_column = None
        data_proccessor = None
        mask_results = None
        all_dates = False
        date_column = 'qwerty'

        extractor = Extractor(start_date, end_date, thread_id, app_settings,
                              filter_by_column, data_proccessor, mask_results,
                              all_dates, date_column)
        url = "https://dev71074.service-now.com/api/now/table/sys_audit2?sysparm_query=tablename=incident"
        params = SimpleNamespace(
            extracting=SimpleNamespace(url=url, batch_size=1000),
        )
        url = extractor._Extractor__get_request_url(False, params, start_date, end_date)
        parsed = parse.parse_qs(parse.urlsplit(url).query)
        assert 'sys_updated_on' not in str(parsed['sysparm_query'])
        assert 'sys_created_on' not in str(parsed['sysparm_query'])
        assert 'qwerty' in str(parsed['sysparm_query'])


    def test_filter_dates_column_all_dates(self):
        foo = lambda msg: None
        start_date = '-'
        end_date = '-'
        thread_id = 0
        app_settings = SimpleNamespace(
            logger=SimpleNamespace(info=foo)
        )
        filter_by_column = None
        data_proccessor = None
        mask_results = None,
        all_dates = True
        date_column = ''

        extractor = Extractor(start_date, end_date, thread_id, app_settings,
                              filter_by_column, data_proccessor, mask_results,
                              all_dates, date_column)
        url = "https://dev71074.service-now.com/api/now/table/sys_audit2?sysparm_query=tablename=incident"
        params = SimpleNamespace(
            extracting=SimpleNamespace(url=url, batch_size=1000),
        )
        url = extractor._Extractor__get_request_url(False, params, start_date, end_date)
        parsed = parse.parse_qs(parse.urlsplit(url).query)
        assert 'sys_updated_on'  not in str(parsed['sysparm_query'])
        assert 'sys_created_on' not in str(parsed['sysparm_query'])

    def test_filter_dates_columnnon_non_sys_audit(self):
        foo = lambda msg: None
        start_date = datetime.strptime("2021-10-03", "%Y-%m-%d").date()
        end_date = datetime.strptime("2021-10-04", "%Y-%m-%d").date()
        thread_id = 0
        app_settings = SimpleNamespace(
            logger=SimpleNamespace(info=foo)
        )
        filter_by_column = None
        data_proccessor = None
        mask_results = None,
        all_dates = False
        date_column = ''

        extractor = Extractor(start_date, end_date, thread_id, app_settings,
                              filter_by_column, data_proccessor, mask_results,
                              all_dates, date_column)
        url = "https://dev71074.service-now.com/api/now/table/sys_audit2?sysparm_query=tablename=incident"
        params = SimpleNamespace(
            extracting=SimpleNamespace(url=url, batch_size=1000),
        )
        url = extractor._Extractor__get_request_url(False, params, start_date, end_date)
        parsed = parse.parse_qs(parse.urlsplit(url).query)
        assert 'sys_updated_on' in str(parsed['sysparm_query'])
        assert 'sys_created_on' not in str(parsed['sysparm_query'])
