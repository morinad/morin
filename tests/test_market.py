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
MRKT_TOKEN = ''
MRKT_CLIENT_ID = ''
# =================================

credentials_filled = bool(MRKT_TOKEN and MRKT_CLIENT_ID)
skip_reason = 'MRKT_TOKEN и MRKT_CLIENT_ID не заполнены — вставьте свои данные в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

BASE_URL = 'https://api.partner.market.yandex.ru'


def get_api_client():
    client = BaseMarketplaceClient(
        base_url=BASE_URL,
        headers={
            'Authorization': f'Bearer {MRKT_TOKEN}',
            'Content-Type': 'application/json'
        }
    )
    return client


# =====================================================================
#  Init-тесты (без credentials, но нужен httpx)
# =====================================================================

@needs_httpx
class TestMRKTInit:
    def test_api_is_base_marketplace_client(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake', 'Content-Type': 'application/json'}
        )
        assert isinstance(api, BaseMarketplaceClient)

    def test_base_url_contains_market(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake'}
        )
        base_url = str(api.client.base_url)
        assert 'api.partner.market.yandex.ru' in base_url

    def test_headers_contain_authorization_bearer(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer test_token', 'Content-Type': 'application/json'}
        )
        headers = dict(api.client.headers)
        assert 'Bearer test_token' in headers.get('authorization', '')

    def test_headers_contain_content_type(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake', 'Content-Type': 'application/json'}
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


@needs_httpx
class TestMRKTSourceDict:
    def _make_obj(self):
        from morin.market_by_date import MRKTbyDate
        obj = MRKTbyDate(
            bot_token='', chats='test', message_type='',
            subd='', host='', port='', username='', password='', database='',
            add_name='test', clientid='12345', token='fake_token',
            start='2025-01-01', backfill_days=0, reports='stocks'
        )
        return obj

    def test_source_dict_has_stocks(self):
        obj = self._make_obj()
        assert 'stocks' in obj.source_dict

    def test_source_dict_has_mappings(self):
        obj = self._make_obj()
        assert 'mappings' in obj.source_dict

    def test_source_dict_has_orders(self):
        obj = self._make_obj()
        assert 'orders' in obj.source_dict

    def test_source_dict_has_price_report(self):
        obj = self._make_obj()
        assert 'price_report' in obj.source_dict

    def test_source_dict_has_orders_report(self):
        obj = self._make_obj()
        assert 'orders_report' in obj.source_dict

    def test_stocks_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['stocks']
        assert cfg['platform'] == 'mrkt'
        assert cfg['refresh_type'] == 'delete_all'
        assert cfg['history'] is False

    def test_orders_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['orders']
        assert cfg['platform'] == 'mrkt'
        assert cfg['merge_type'] == 'ReplacingMergeTree(timeStamp)'
        assert cfg['history'] is True

    def test_price_report_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['price_report']
        assert cfg['platform'] == 'mrkt'
        assert cfg['refresh_type'] == 'delete_date'
        assert cfg['history'] is True


# =====================================================================
#  Live-тесты: остатки на складе
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetStocksData:
    def test_returns_tuple(self):
        api = get_api_client()
        payload = {"limit": 10}
        try:
            import httpx
            try:
                result = api._request('POST', f'/campaigns/{MRKT_CLIENT_ID}/offers/stocks', json=payload)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (400, 404):
                    pytest.skip("Кампания не поддерживает остатки (400/404)")
                raise
        except ImportError:
            pytest.skip("httpx не установлен")
        assert 'result' in result
        warehouses = result['result'].get('warehouses', [])
        assert isinstance(warehouses, list)
        print(f"\nСкладов: {len(warehouses)}")

    def test_warehouse_has_offers(self):
        api = get_api_client()
        payload = {"limit": 10}
        try:
            import httpx
            try:
                result = api._request('POST', f'/campaigns/{MRKT_CLIENT_ID}/offers/stocks', json=payload)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (400, 404):
                    pytest.skip("Кампания не поддерживает остатки (400/404)")
                raise
        except ImportError:
            pytest.skip("httpx не установлен")
        warehouses = result['result'].get('warehouses', [])
        if not warehouses:
            pytest.skip("Нет складов")
        wh = warehouses[0]
        assert 'warehouseId' in wh
        assert 'offers' in wh
        print(f"\nСклад {wh['warehouseId']}: офферов={len(wh.get('offers', []))}")


# =====================================================================
#  Live-тесты: маппинги офферов
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetOfferMappings:
    def test_returns_offer_mappings(self):
        api = get_api_client()
        params = {"limit": 10}
        result = api._request('POST', f'/businesses/{MRKT_CLIENT_ID}/offer-mappings', params=params)
        assert 'result' in result
        mappings = result['result'].get('offerMappings', [])
        assert isinstance(mappings, list)
        print(f"\nМаппингов: {len(mappings)}")

    def test_mapping_has_offer(self):
        api = get_api_client()
        params = {"limit": 10}
        result = api._request('POST', f'/businesses/{MRKT_CLIENT_ID}/offer-mappings', params=params)
        mappings = result['result'].get('offerMappings', [])
        if not mappings:
            pytest.skip("Нет маппингов")
        m = mappings[0]
        assert 'offer' in m
        offer = m['offer']
        assert 'offerId' in offer
        print(f"\nПервый оффер: offerId={offer['offerId']}, name={offer.get('name', '')[:50]}")


# =====================================================================
#  Live-тесты: заказы
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetOrdersData:
    def test_returns_orders(self):
        api = get_api_client()
        payload = {'dateFrom': YESTERDAY, 'dateTo': YESTERDAY}
        params = {"limit": 10}
        result = api._request('POST', f'/campaigns/{MRKT_CLIENT_ID}/stats/orders', json=payload, params=params)
        assert 'result' in result
        orders = result['result'].get('orders', [])
        assert isinstance(orders, list)
        print(f"\nЗаказов за {YESTERDAY}: {len(orders)}")

    def test_order_has_id(self):
        api = get_api_client()
        payload = {'dateFrom': WEEK_AGO, 'dateTo': YESTERDAY}
        params = {"limit": 10}
        result = api._request('POST', f'/campaigns/{MRKT_CLIENT_ID}/stats/orders', json=payload, params=params)
        orders = result['result'].get('orders', [])
        if not orders:
            pytest.skip("Нет заказов за период")
        o = orders[0]
        assert 'id' in o
        print(f"\nПервый заказ: id={o['id']}, status={o.get('status', '')}")


# =====================================================================
#  Live-тесты: генерация отчётов
# =====================================================================

@needs_credentials
@needs_httpx
class TestGenerateReport:
    def test_generate_price_report_returns_id(self):
        api = get_api_client()
        from morin.common import Common
        common = Common('', [''], '')
        first_date = common.get_month_start(YESTERDAY)
        year = first_date.split('-')[0]
        month = first_date.split('-')[1]
        params = {"format": "CSV", "language": "EN"}
        payload = {
            "businessId": MRKT_CLIENT_ID,
            "monthFrom": month, "monthTo": month,
            "yearFrom": year, "yearTo": year
        }
        result = api._request('POST', '/reports/united-marketplace-services/generate', json=payload, params=params)
        assert 'result' in result
        report_id = result['result'].get('reportId')
        assert report_id is not None
        print(f"\nreportId: {report_id}")

    def test_generate_orders_report_returns_id(self):
        api = get_api_client()
        from morin.common import Common
        common = Common('', [''], '')
        first_date = common.get_month_start(YESTERDAY)
        params = {"format": "CSV", "language": "EN"}
        payload = {
            "businessId": MRKT_CLIENT_ID,
            "dateFrom": first_date, "dateTo": YESTERDAY
        }
        result = api._request('POST', '/reports/united-orders/generate', json=payload, params=params)
        assert 'result' in result
        report_id = result['result'].get('reportId')
        assert report_id is not None
        print(f"\nreportId: {report_id}")

    def test_check_report_status(self):
        api = get_api_client()
        from morin.common import Common
        common = Common('', [''], '')
        first_date = common.get_month_start(YESTERDAY)
        year = first_date.split('-')[0]
        month = first_date.split('-')[1]
        params = {"format": "CSV", "language": "EN"}
        payload = {
            "businessId": MRKT_CLIENT_ID,
            "monthFrom": month, "monthTo": month,
            "yearFrom": year, "yearTo": year
        }
        result = api._request('POST', '/reports/united-marketplace-services/generate', json=payload, params=params)
        report_id = result['result'].get('reportId')
        time.sleep(10)
        status_result = api._request('GET', f'/reports/info/{report_id}')
        assert 'result' in status_result
        file_url = status_result['result'].get('file')
        print(f"\nreportId: {report_id}, file: {file_url}")


# =====================================================================
#  Ручной запуск: python tests/test_market.py
# =====================================================================
if __name__ == '__main__':
    DELAY_SECONDS = 60

    METHODS_TO_TEST = [
        'get_stocks',
        'get_mappings',
        'get_orders',
        'generate_price_report',
        'generate_orders_report',
    ]

    def run_get_stocks():
        api = get_api_client()
        payload = {"limit": 100}
        result = api._request('POST', f'/campaigns/{MRKT_CLIENT_ID}/offers/stocks', json=payload)
        warehouses = result['result'].get('warehouses', [])
        print(f"  Складов: {len(warehouses)}")
        total_offers = 0
        for wh in warehouses:
            offers = wh.get('offers', [])
            total_offers += len(offers)
            print(f"  Склад {wh['warehouseId']}: офферов={len(offers)}")
        print(f"  Всего офферов: {total_offers}")

    def run_get_mappings():
        api = get_api_client()
        all_mappings = []
        next_page_token = None
        for _ in range(10):
            params = {"limit": 200}
            if next_page_token:
                params["page_token"] = next_page_token
            result = api._request('POST', f'/businesses/{MRKT_CLIENT_ID}/offer-mappings', params=params)
            mappings = result['result'].get('offerMappings', [])
            all_mappings.extend(mappings)
            next_page_token = result['result'].get('paging', {}).get('nextPageToken')
            if not next_page_token:
                break
            time.sleep(0.5)
        print(f"  Маппингов: {len(all_mappings)}")
        for m in all_mappings[:5]:
            offer = m.get('offer', {})
            print(f"  offerId={offer.get('offerId','')}, name={offer.get('name','')[:50]}")

    def run_get_orders():
        api = get_api_client()
        payload = {'dateFrom': WEEK_AGO, 'dateTo': YESTERDAY}
        params = {"limit": 50}
        result = api._request('POST', f'/campaigns/{MRKT_CLIENT_ID}/stats/orders', json=payload, params=params)
        orders = result['result'].get('orders', [])
        print(f"  Заказов за {WEEK_AGO} — {YESTERDAY}: {len(orders)}")
        for o in orders[:5]:
            print(f"  id={o.get('id','')}, status={o.get('status','')}")

    def run_generate_price_report():
        from morin.common import Common
        common = Common('', [''], '')
        first_date = common.get_month_start(YESTERDAY)
        year = first_date.split('-')[0]
        month = first_date.split('-')[1]
        api = get_api_client()
        params = {"format": "CSV", "language": "EN"}
        payload = {
            "businessId": MRKT_CLIENT_ID,
            "monthFrom": month, "monthTo": month,
            "yearFrom": year, "yearTo": year
        }
        result = api._request('POST', '/reports/united-marketplace-services/generate', json=payload, params=params)
        report_id = result['result']['reportId']
        print(f"  reportId: {report_id}")
        print(f"  Ожидание готовности отчёта...")
        for k in range(30):
            time.sleep(10)
            status = api._request('GET', f'/reports/info/{report_id}')
            file_url = status['result'].get('file')
            print(f"  Попытка {k+1}: file={file_url}")
            if file_url and 'http' in str(file_url):
                print(f"  Отчёт готов: {file_url}")
                break

    def run_generate_orders_report():
        from morin.common import Common
        common = Common('', [''], '')
        first_date = common.get_month_start(YESTERDAY)
        api = get_api_client()
        params = {"format": "CSV", "language": "EN"}
        payload = {
            "businessId": MRKT_CLIENT_ID,
            "dateFrom": first_date, "dateTo": YESTERDAY
        }
        result = api._request('POST', '/reports/united-orders/generate', json=payload, params=params)
        report_id = result['result']['reportId']
        print(f"  reportId: {report_id}")
        print(f"  Ожидание готовности отчёта...")
        for k in range(30):
            time.sleep(10)
            status = api._request('GET', f'/reports/info/{report_id}')
            file_url = status['result'].get('file')
            print(f"  Попытка {k+1}: file={file_url}")
            if file_url and 'http' in str(file_url):
                print(f"  Отчёт готов: {file_url}")
                break

    if not MRKT_TOKEN or not MRKT_CLIENT_ID:
        print("MRKT_TOKEN / MRKT_CLIENT_ID не заполнены!")
        sys.exit(1)

    for method in METHODS_TO_TEST:
        print(f"\n{'='*60}")
        print(f"  {method}")
        print(f"{'='*60}")
        try:
            if method == 'get_stocks':
                run_get_stocks()
            elif method == 'get_mappings':
                run_get_mappings()
            elif method == 'get_orders':
                run_get_orders()
            elif method == 'generate_price_report':
                run_generate_price_report()
            elif method == 'generate_orders_report':
                run_generate_orders_report()
            print(f"  ✓ OK")
        except Exception as e:
            print(f"  ✗ ОШИБКА: {e}")
        if method != METHODS_TO_TEST[-1]:
            print(f"\n  Пауза {DELAY_SECONDS} сек...")
            time.sleep(DELAY_SECONDS)
