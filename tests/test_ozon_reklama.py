import sys
import os
import time
import pytest
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    import httpx
    from morin.base_client import BaseMarketplaceClient
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

needs_httpx = pytest.mark.skipif(not HAS_HTTPX, reason='httpx не установлен — используйте старую версию без httpx')

# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
OZON_ADS_CLIENT_ID = ''
OZON_ADS_CLIENT_SECRET = ''
# =================================

credentials_filled = bool(OZON_ADS_CLIENT_ID and OZON_ADS_CLIENT_SECRET)
skip_reason = 'OZON_ADS_CLIENT_ID и OZON_ADS_CLIENT_SECRET не заполнены — вставьте свои данные в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

BASE_URL = 'https://api-performance.ozon.ru'


def get_api_client():
    client = BaseMarketplaceClient(
        base_url=BASE_URL,
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
    )
    payload = {"client_id": OZON_ADS_CLIENT_ID, "client_secret": OZON_ADS_CLIENT_SECRET, "grant_type": "client_credentials"}
    result = client._request('POST', '/api/client/token', json=payload)
    token = result['access_token']
    client.client.headers['Authorization'] = f'Bearer {token}'
    return client


# =====================================================================
#  Init-тесты (без credentials)
# =====================================================================

@needs_httpx
class TestOzonReklamaInit:
    def test_api_is_base_marketplace_client(self):
        from morin.ozon_reklama import OZONreklama
        class FakeCommon:
            def log_func(self, *args): pass
            def transliterate_key(self, x): return x
        obj = object.__new__(OZONreklama)
        obj.bot_token = ''
        obj.chat_list = ''
        obj.message_type = ''
        obj.clientid = 'fake_id'
        obj.token = 'fake_secret'
        obj.add_name = 'test'
        obj.common = FakeCommon()
        obj.now = datetime.now()
        obj.err429 = False
        obj.api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )
        assert isinstance(obj.api, BaseMarketplaceClient)

    def test_base_url_contains_performance(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Content-Type': 'application/json'}
        )
        base_url = str(api.client.base_url)
        assert 'api-performance.ozon.ru' in base_url

    def test_headers_contain_content_type(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )
        headers = dict(api.client.headers)
        assert headers.get('content-type') == 'application/json'

    def test_err429_default_false(self):
        api = BaseMarketplaceClient(base_url=BASE_URL)
        assert api.err429 is False

    def test_api_has_request_method(self):
        api = BaseMarketplaceClient(base_url=BASE_URL)
        assert hasattr(api, '_request') and callable(api._request)

    def test_api_has_request_raw_method(self):
        api = BaseMarketplaceClient(base_url=BASE_URL)
        assert hasattr(api, '_request_raw') and callable(api._request_raw)


# =====================================================================
#  text_to_df — юнит-тест парсинга CSV
# =====================================================================

@needs_httpx
class TestTextToDf:
    def _make_obj(self):
        from morin.ozon_reklama import OZONreklama
        class FakeCommon:
            def log_func(self, *args): pass
        obj = object.__new__(OZONreklama)
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        obj.now = datetime.now()
        return obj

    def test_sp_report_detected(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №12345, Трафареты\n"
            "Дата;ID заказа;Номер заказа;Ozon ID;Ozon ID продвигаемого товара;Артикул;Наименование;Количество;Цена продажи;Стоимость, ₽;Ставка, ₽;Расход, ₽\n"
            "2025-01-15;ord1;num1;oz1;poz1;art1;Товар 1;2;500;1000;10;20\n"
            "Всего;-;-;-;-;-;-;2;500;1000;10;20"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert add_to_table == 'sp'
        assert len(rows) == 1
        assert rows[0]['orderId'] == 'ord1'
        assert str(rows[0]['id']) == '12345'

    def test_sku_report_detected(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №99999, Продвижение\n"
            "Дата добавления;SKU;Название товара;Цена товара, ₽;Показы;Клики;Расход, ₽, с НДС;В корзину;Продажи, ₽;Заказы;Заказы модели;Продажи с заказов модели, ₽;ДРР, %\n"
            "2025-01-15;sku1;Товар;100;50;5;15;3;200;2;1;100;7.5\n"
            "Всего;-;-;-;50;5;15;3;200;2;1;100;7.5"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert add_to_table == 'sku'
        assert len(rows) == 1
        assert rows[0]['sku'] == 'sku1'

    def test_banner_report_detected(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №77777, Баннер\n"
            "Баннер;Тип страницы;Условие показа;Платформа;Показы;Клики;Охват;Расход, ₽, с НДС\n"
            "banner1;page1;cond1;desktop;1000;50;800;150\n"
            "Всего;-;-;-;1000;50;800;150"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert add_to_table == 'banner'
        assert len(rows) == 1
        assert rows[0]['banner'] == 'banner1'

    def test_shelf_report_detected(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №55555, Полка\n"
            "Тип условия;Условие показа;Платформа;Показы;Клики;Охват;Расход, ₽, с НДС\n"
            "type1;cond1;mobile;500;25;400;75\n"
            "Всего;-;-;500;25;400;75"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert add_to_table == 'shelf'
        assert len(rows) == 1
        assert rows[0]['conditionType'] == 'type1'

    def test_sis_report_detected(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №33333, SIS\n"
            "Тип страницы;Показы;Клики;Расход, ₽, с НДС;Охват\n"
            "search;200;10;30;180\n"
            "Всего;200;10;30;180"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert add_to_table == 'sis'
        assert len(rows) == 1
        assert rows[0]['pageType'] == 'search'

    def test_empty_csv_returns_empty_list(self):
        obj = self._make_obj()
        csv_text = "Кампания №12345, Пустой отчёт"
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert rows == []
        assert add_to_table == 'unknown'

    def test_date_and_id_added(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №12345, SIS\n"
            "Тип страницы;Показы;Клики;Расход, ₽, с НДС;Охват\n"
            "search;200;10;30;180"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert len(rows) == 1
        assert str(rows[0]['id']) == '12345'
        assert 'date' in rows[0]

    def test_correction_row_filtered(self):
        obj = self._make_obj()
        csv_text = (
            "Кампания №33333, SIS\n"
            "Тип страницы;Показы;Клики;Расход, ₽, с НДС;Охват\n"
            "search;200;10;30;180\n"
            "Корректировка;0;0;-5;0\n"
            "Всего;200;10;25;180"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        assert len(rows) == 1
        assert rows[0]['pageType'] == 'search'


@needs_httpx
class TestInsertWithAutoColumns:
    def test_uniq_map_keys(self):
        from morin.ozon_reklama import OZONreklama
        obj = object.__new__(OZONreklama)
        uniq_map = {
            'sp': 'date,id,orderDate,orderId,ozonId,productOzonId,artikul',
            'sku': 'date,id,sku',
            'banner': 'date,id,banner,pageType,viewCond,platform',
            'shelf': 'date,id,conditionType,viewCond,platform',
            'sis': 'date,id,pageType',
        }
        for report_type, expected in uniq_map.items():
            assert expected == uniq_map[report_type]


# =====================================================================
#  Live-тесты: получение токена
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetToken:
    def test_returns_access_token(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )
        payload = {"client_id": OZON_ADS_CLIENT_ID, "client_secret": OZON_ADS_CLIENT_SECRET, "grant_type": "client_credentials"}
        result = api._request('POST', '/api/client/token', json=payload)
        assert 'access_token' in result
        assert len(result['access_token']) > 0
        print(f"\nТокен получен, длина: {len(result['access_token'])}")

    def test_token_type_is_bearer(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )
        payload = {"client_id": OZON_ADS_CLIENT_ID, "client_secret": OZON_ADS_CLIENT_SECRET, "grant_type": "client_credentials"}
        result = api._request('POST', '/api/client/token', json=payload)
        assert result.get('token_type', '').lower() == 'bearer'


# =====================================================================
#  Live-тесты: получение списка кампаний
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetCampaigns:
    def test_returns_list(self):
        api = get_api_client()
        result = api._request('GET', '/api/client/campaign')
        assert 'list' in result
        assert isinstance(result['list'], list)
        print(f"\nКампаний: {len(result['list'])}")

    def test_campaign_has_required_fields(self):
        api = get_api_client()
        result = api._request('GET', '/api/client/campaign')
        campaigns = result['list']
        if len(campaigns) == 0:
            pytest.skip("Нет кампаний для теста")
        c = campaigns[0]
        assert 'id' in c
        assert 'title' in c
        assert 'state' in c
        assert 'advObjectType' in c
        assert 'createdAt' in c
        print(f"\nПервая кампания: id={c['id']}, title={c['title']}, state={c['state']}")

    def test_campaign_has_dates(self):
        api = get_api_client()
        result = api._request('GET', '/api/client/campaign')
        campaigns = result['list']
        if len(campaigns) == 0:
            pytest.skip("Нет кампаний для теста")
        c = campaigns[0]
        assert 'fromDate' in c or 'createdAt' in c
        assert 'toDate' in c or 'updatedAt' in c
        print(f"\ncreatedAt={c.get('createdAt')}, fromDate={c.get('fromDate')}, toDate={c.get('toDate')}")


# =====================================================================
#  Live-тесты: создание отчёта статистики
# =====================================================================

@needs_credentials
@needs_httpx
class TestStatisticsReport:
    def _get_campaign_ids(self, api):
        result = api._request('GET', '/api/client/campaign')
        campaigns = result.get('list', [])
        return [str(c['id']) for c in campaigns[:2]]

    def test_post_statistics_returns_uuid(self):
        api = get_api_client()
        campaign_ids = self._get_campaign_ids(api)
        if not campaign_ids:
            pytest.skip("Нет кампаний для теста")
        time.sleep(5)
        payload = {
            "campaigns": campaign_ids,
            "dateFrom": WEEK_AGO,
            "dateTo": YESTERDAY,
            "groupBy": "NO_GROUP_BY"
        }
        result = api._request('POST', '/api/client/statistics', json=payload)
        assert 'UUID' in result
        assert len(result['UUID']) > 0
        print(f"\nUUID отчёта: {result['UUID']}")


# =====================================================================
#  Ручной запуск: python tests/test_ozon_reklama.py
# =====================================================================
if __name__ == '__main__':
    DELAY_SECONDS = 60

    METHODS_TO_TEST = [
        'get_token',
        'get_campaigns',
        'statistics_report',
        'text_to_df',
    ]

    def run_get_token():
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )
        payload = {"client_id": OZON_ADS_CLIENT_ID, "client_secret": OZON_ADS_CLIENT_SECRET, "grant_type": "client_credentials"}
        result = api._request('POST', '/api/client/token', json=payload)
        print(f"  access_token длина: {len(result.get('access_token', ''))}")
        print(f"  token_type: {result.get('token_type')}")
        print(f"  expires_in: {result.get('expires_in')}")
        return result['access_token']

    def run_get_campaigns(token):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        )
        result = api._request('GET', '/api/client/campaign')
        campaigns = result['list']
        print(f"  Кампаний: {len(campaigns)}")
        for c in campaigns[:5]:
            print(f"  id={c['id']}, title={c.get('title','')}, state={c.get('state','')}, type={c.get('advObjectType','')}")
        return [str(c['id']) for c in campaigns]

    def run_statistics_report(token, campaign_ids):
        if not campaign_ids:
            print("  Нет кампаний — пропуск")
            return
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        )
        payload = {
            "campaigns": campaign_ids[:2],
            "dateFrom": WEEK_AGO,
            "dateTo": YESTERDAY,
            "groupBy": "NO_GROUP_BY"
        }
        result = api._request('POST', '/api/client/statistics', json=payload)
        print(f"  UUID: {result.get('UUID')}")
        uuid = result['UUID']
        print(f"  Ожидание готовности отчёта...")
        for k in range(30):
            time.sleep(10)
            try:
                status = api._request('GET', f'/api/client/statistics/{uuid}')
                state = status.get('state', '')
                print(f"  Попытка {k+1}: state={state}")
                if state == 'OK':
                    break
            except:
                pass
        response = api._request_raw('GET', '/api/client/statistics/report', params={'UUID': uuid})
        print(f"  status: {response.status_code}")
        print(f"  content length: {len(response.content)}")
        text = response.text
        lines = text.splitlines()
        print(f"  Строк в ответе: {len(lines)}")
        if len(lines) > 0:
            print(f"  Первая строка: {lines[0][:200]}")
        if len(lines) > 1:
            print(f"  Вторая строка: {lines[1][:200]}")

    def run_text_to_df():
        from morin.ozon_reklama import OZONreklama
        class FakeCommon:
            def log_func(self, *args): pass
        obj = object.__new__(OZONreklama)
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        obj.now = datetime.now()

        csv_text = (
            "Кампания №12345, SIS\n"
            "Тип страницы;Показы;Клики;Расход, ₽, с НДС;Охват\n"
            "search;200;10;30;180\n"
            "Корректировка;0;0;-5;0\n"
            "Всего;200;10;25;180"
        )
        rows, add_to_table = obj.text_to_df(csv_text, '2025-01-15')
        print(f"  report_type: {add_to_table}")
        print(f"  rows: {len(rows)}")
        if rows:
            print(f"  keys: {list(rows[0].keys())}")
            print(f"  data: {rows[0]}")

    if not OZON_ADS_CLIENT_ID or not OZON_ADS_CLIENT_SECRET:
        print("OZON_ADS_CLIENT_ID / OZON_ADS_CLIENT_SECRET не заполнены!")
        print("Запускаю только юнит-тесты...\n")
        METHODS_TO_TEST = ['text_to_df']

    token = None
    campaign_ids = None
    for method in METHODS_TO_TEST:
        print(f"\n{'='*60}")
        print(f"  {method}")
        print(f"{'='*60}")
        try:
            if method == 'get_token':
                token = run_get_token()
            elif method == 'get_campaigns':
                campaign_ids = run_get_campaigns(token)
            elif method == 'statistics_report':
                run_statistics_report(token, campaign_ids)
            elif method == 'text_to_df':
                run_text_to_df()
            print(f"  ✓ OK")
        except Exception as e:
            print(f"  ✗ ОШИБКА: {e}")
        if method != METHODS_TO_TEST[-1] and method != 'text_to_df':
            print(f"\n  Пауза {DELAY_SECONDS} сек...")
            time.sleep(DELAY_SECONDS)
