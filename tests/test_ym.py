import sys
import os
import time
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from morin.base_client import BaseMarketplaceClient
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

needs_httpx = pytest.mark.skipif(not HAS_HTTPX, reason='httpx не установлен — используйте старую версию без httpx')


# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
YM_TOKEN = ''
YM_COUNTER_ID = ''
# =================================

credentials_filled = bool(YM_TOKEN and YM_COUNTER_ID)
skip_reason = 'YM_TOKEN и YM_COUNTER_ID не заполнены — вставьте свои данные в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

BASE_URL = 'https://api-metrika.yandex.ru'


def get_api_client():
    client = BaseMarketplaceClient(
        base_url=BASE_URL,
        headers={'Authorization': f'OAuth {YM_TOKEN}'}
    )
    return client


# =====================================================================
#  Init-тесты (без credentials, но нужен httpx)
# =====================================================================

@needs_httpx
class TestYMInit:
    def test_api_is_base_marketplace_client(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'OAuth fake_token'}
        )
        assert isinstance(api, BaseMarketplaceClient)

    def test_base_url_contains_metrika(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'OAuth fake'}
        )
        base_url = str(api.client.base_url)
        assert 'api-metrika.yandex.ru' in base_url

    def test_headers_contain_authorization_oauth(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'OAuth test_token'}
        )
        headers = dict(api.client.headers)
        assert 'OAuth test_token' in headers.get('authorization', '')

    def test_err429_default_false(self):
        api = BaseMarketplaceClient(base_url=BASE_URL)
        assert api.err429 is False

    def test_api_has_request_method(self):
        api = BaseMarketplaceClient(base_url=BASE_URL)
        assert hasattr(api, '_request') and callable(api._request)

    def test_api_has_request_raw_method(self):
        api = BaseMarketplaceClient(base_url=BASE_URL)
        assert hasattr(api, '_request_raw') and callable(api._request_raw)


@needs_httpx
class TestYMSourceDict:
    def _make_obj(self):
        from morin.ym_by_date import YMbyDate
        obj = YMbyDate(
            bot_token='', chats='test', message_type='',
            subd='', host='', port='', username='', password='', database='',
            add_name='test', login='12345678', token='fake_token',
            start='2025-01-01', backfill_days=0,
            report='date', dimensions='ym:s:date', metrics='ym:s:visits'
        )
        return obj

    def test_source_dict_has_date(self):
        obj = self._make_obj()
        assert 'date' in obj.source_dict

    def test_source_dict_has_nodate(self):
        obj = self._make_obj()
        assert 'nodate' in obj.source_dict

    def test_date_report_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['date']
        assert cfg['platform'] == 'ym'
        assert cfg['refresh_type'] == 'delete_date'
        assert cfg['history'] is True

    def test_nodate_report_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['nodate']
        assert cfg['platform'] == 'ym'
        assert cfg['refresh_type'] == 'delete_all'
        assert cfg['history'] is False


# =====================================================================
#  Юнит-тест: fetch_all_metrika_data — трансформация данных
# =====================================================================

@needs_httpx
class TestFetchAllMetrikaDataTransform:
    def _make_obj(self):
        from morin.ym_by_date import YMbyDate
        obj = object.__new__(YMbyDate)
        class FakeCommon:
            def log_func(self, *args): pass
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        obj.dimensions = 'ym:s:date,ym:s:TrafficSource'
        obj.metrics = 'ym:s:visits,ym:s:pageviews'
        obj.filters = None
        obj.login = '12345678'
        obj.err429 = False
        return obj

    def test_transforms_dimensions_and_metrics(self):
        obj = self._make_obj()
        mock_api_response = {
            "data": [
                {
                    "dimensions": [{"name": "2025-01-15"}, {"name": "organic"}],
                    "metrics": [100.0, 350.0]
                },
                {
                    "dimensions": [{"name": "2025-01-15"}, {"name": "direct"}],
                    "metrics": [50.0, 120.0]
                }
            ]
        }

        class MockApi:
            err429 = False
            def _request(self, method, path, **kwargs):
                return mock_api_response

        obj.api = MockApi()
        result = obj.fetch_all_metrika_data('2025-01-15', '2025-01-15')
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['date'] == '2025-01-15'
        assert result[0]['TrafficSource'] == 'organic'
        assert result[0]['visits'] == 100.0
        assert result[0]['pageviews'] == 350.0
        assert result[1]['TrafficSource'] == 'direct'
        assert result[1]['visits'] == 50.0

    def test_empty_data_returns_empty_list(self):
        obj = self._make_obj()

        class MockApi:
            err429 = False
            def _request(self, method, path, **kwargs):
                return {"data": []}

        obj.api = MockApi()
        result = obj.fetch_all_metrika_data('2025-01-15', '2025-01-15')
        assert result == []

    def test_single_dimension_and_metric(self):
        obj = self._make_obj()
        obj.dimensions = 'ym:s:date'
        obj.metrics = 'ym:s:visits'

        class MockApi:
            err429 = False
            def _request(self, method, path, **kwargs):
                return {"data": [
                    {"dimensions": [{"name": "2025-01-15"}], "metrics": [42.0]}
                ]}

        obj.api = MockApi()
        result = obj.fetch_all_metrika_data('2025-01-15', '2025-01-15')
        assert len(result) == 1
        assert result[0]['date'] == '2025-01-15'
        assert result[0]['visits'] == 42.0


# =====================================================================
#  Live-тесты: получение данных Метрики
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetMetrikaData:
    def test_returns_data(self):
        api = get_api_client()
        params = {
            "ids": YM_COUNTER_ID,
            "metrics": "ym:s:visits,ym:s:pageviews",
            "dimensions": "ym:s:date",
            "date1": WEEK_AGO,
            "date2": YESTERDAY,
            "limit": 10,
            "accuracy": "full"
        }
        result = api._request('GET', '/stat/v1/data', params=params)
        assert 'data' in result
        assert isinstance(result['data'], list)
        print(f"\nСтрок данных: {len(result['data'])}")
        if result['data']:
            row = result['data'][0]
            assert 'dimensions' in row
            assert 'metrics' in row
            print(f"Первая строка: dimensions={row['dimensions']}, metrics={row['metrics']}")

    def test_total_rows_present(self):
        api = get_api_client()
        params = {
            "ids": YM_COUNTER_ID,
            "metrics": "ym:s:visits",
            "dimensions": "ym:s:date",
            "date1": WEEK_AGO,
            "date2": YESTERDAY,
            "limit": 10,
            "accuracy": "full"
        }
        result = api._request('GET', '/stat/v1/data', params=params)
        assert 'total_rows' in result or 'data' in result
        print(f"\ntotal_rows: {result.get('total_rows', 'N/A')}")

    def test_with_traffic_source_dimension(self):
        api = get_api_client()
        params = {
            "ids": YM_COUNTER_ID,
            "metrics": "ym:s:visits,ym:s:bounceRate",
            "dimensions": "ym:s:date,ym:s:lastTrafficSource",
            "date1": YESTERDAY,
            "date2": YESTERDAY,
            "limit": 10,
            "accuracy": "full"
        }
        result = api._request('GET', '/stat/v1/data', params=params)
        assert 'data' in result
        data = result['data']
        if len(data) > 0:
            row = data[0]
            assert len(row['dimensions']) == 2
            assert len(row['metrics']) == 2
            print(f"\nИсточники трафика: {len(data)} строк")
            for r in data[:3]:
                print(f"  date={r['dimensions'][0]['name']}, source={r['dimensions'][1]['name']}, visits={r['metrics'][0]}")


# =====================================================================
#  Ручной запуск: python tests/test_ym.py
# =====================================================================
if __name__ == '__main__':
    DELAY_SECONDS = 60

    METHODS_TO_TEST = [
        'transform_data',
        'get_metrika_data',
        'get_metrika_traffic_sources',
    ]

    def run_transform_data():
        from morin.ym_by_date import YMbyDate
        class FakeCommon:
            def log_func(self, *args): pass
        obj = object.__new__(YMbyDate)
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        obj.dimensions = 'ym:s:date,ym:s:TrafficSource'
        obj.metrics = 'ym:s:visits,ym:s:pageviews'
        obj.filters = None
        obj.login = '12345678'
        obj.err429 = False

        class MockApi:
            err429 = False
            def _request(self, method, path, **kwargs):
                return {"data": [
                    {"dimensions": [{"name": "2025-01-15"}, {"name": "organic"}], "metrics": [100.0, 350.0]}
                ]}
        obj.api = MockApi()
        result = obj.fetch_all_metrika_data('2025-01-15', '2025-01-15')
        print(f"  rows: {len(result)}")
        print(f"  data: {result}")

    def run_get_metrika_data():
        api = get_api_client()
        params = {
            "ids": YM_COUNTER_ID,
            "metrics": "ym:s:visits,ym:s:pageviews",
            "dimensions": "ym:s:date",
            "date1": WEEK_AGO,
            "date2": YESTERDAY,
            "limit": 10,
            "accuracy": "full"
        }
        result = api._request('GET', '/stat/v1/data', params=params)
        data = result.get('data', [])
        print(f"  Строк: {len(data)}")
        for row in data[:5]:
            print(f"  date={row['dimensions'][0]['name']}, visits={row['metrics'][0]}, pageviews={row['metrics'][1]}")

    def run_get_metrika_traffic_sources():
        api = get_api_client()
        params = {
            "ids": YM_COUNTER_ID,
            "metrics": "ym:s:visits,ym:s:bounceRate",
            "dimensions": "ym:s:date,ym:s:lastTrafficSource",
            "date1": YESTERDAY,
            "date2": YESTERDAY,
            "limit": 20,
            "accuracy": "full"
        }
        result = api._request('GET', '/stat/v1/data', params=params)
        data = result.get('data', [])
        print(f"  Строк: {len(data)}")
        for row in data[:10]:
            print(f"  date={row['dimensions'][0]['name']}, source={row['dimensions'][1]['name']}, visits={row['metrics'][0]}")

    if not YM_TOKEN or not YM_COUNTER_ID:
        print("YM_TOKEN / YM_COUNTER_ID не заполнены!")
        print("Запускаю только юнит-тесты...\n")
        METHODS_TO_TEST = ['transform_data']

    for method in METHODS_TO_TEST:
        print(f"\n{'='*60}")
        print(f"  {method}")
        print(f"{'='*60}")
        try:
            if method == 'transform_data':
                run_transform_data()
            elif method == 'get_metrika_data':
                run_get_metrika_data()
            elif method == 'get_metrika_traffic_sources':
                run_get_metrika_traffic_sources()
            print(f"  ✓ OK")
        except Exception as e:
            print(f"  ✗ ОШИБКА: {e}")
        if method != METHODS_TO_TEST[-1] and method != 'transform_data':
            print(f"\n  Пауза {DELAY_SECONDS} сек...")
            time.sleep(DELAY_SECONDS)
