import sys
import os
import time
import pytest
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from morin.base_client import BaseMarketplaceClient
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

needs_httpx = pytest.mark.skipif(not HAS_HTTPX, reason='httpx не установлен — используйте старую версию без httpx')


# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
YD_TOKEN = ''
YD_LOGIN = ''
# =================================

credentials_filled = bool(YD_TOKEN and YD_LOGIN)
skip_reason = 'YD_TOKEN и YD_LOGIN не заполнены — вставьте свои данные в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

BASE_URL = 'https://api.direct.yandex.com'


def get_api_client():
    client = BaseMarketplaceClient(
        base_url=BASE_URL,
        headers={
            'Authorization': f'Bearer {YD_TOKEN}',
            'Client-Login': YD_LOGIN,
            'Accept-Language': 'ru',
        }
    )
    return client


# =====================================================================
#  Init-тесты (без credentials, но нужен httpx)
# =====================================================================

@needs_httpx
class TestYDInit:
    def test_api_is_base_marketplace_client(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake', 'Client-Login': 'fake'}
        )
        assert isinstance(api, BaseMarketplaceClient)

    def test_base_url_contains_direct(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake'}
        )
        base_url = str(api.client.base_url)
        assert 'api.direct.yandex.com' in base_url

    def test_headers_contain_authorization_bearer(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer test_token', 'Client-Login': 'test_login'}
        )
        headers = dict(api.client.headers)
        assert 'Bearer test_token' in headers.get('authorization', '')

    def test_headers_contain_client_login(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake', 'Client-Login': 'test_login'}
        )
        headers = dict(api.client.headers)
        assert headers.get('client-login') == 'test_login'

    def test_headers_contain_accept_language(self):
        api = BaseMarketplaceClient(
            base_url=BASE_URL,
            headers={'Authorization': 'Bearer fake', 'Accept-Language': 'ru'}
        )
        headers = dict(api.client.headers)
        assert headers.get('accept-language') == 'ru'

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
class TestYDSourceDict:
    def _make_obj(self):
        from morin.yd_by_date import YDbyDate
        obj = YDbyDate(
            bot_token='', chats='test', message_type='',
            subd='', host='', port='', username='', password='', database='',
            add_name='test', login='fake_login', token='fake_token',
            start='2025-01-01', backfill_days=0,
            columns='Date,Clicks,Cost', report='date'
        )
        return obj

    def test_source_dict_has_date(self):
        obj = self._make_obj()
        assert 'date' in obj.source_dict

    def test_source_dict_has_nodate(self):
        obj = self._make_obj()
        assert 'nodate' in obj.source_dict

    def test_source_dict_has_ads(self):
        obj = self._make_obj()
        assert 'ads' in obj.source_dict

    def test_date_report_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['date']
        assert cfg['platform'] == 'yd'
        assert cfg['refresh_type'] == 'delete_date'
        assert cfg['history'] is True

    def test_ads_report_config(self):
        obj = self._make_obj()
        cfg = obj.source_dict['ads']
        assert cfg['platform'] == 'yd'
        assert cfg['merge_type'] == 'ReplacingMergeTree(timeStamp)'
        assert cfg['refresh_type'] == 'nothing'


# =====================================================================
#  Юнит-тест: tsv_to_dict
# =====================================================================

@needs_httpx
class TestTsvToDict:
    def _make_obj(self):
        from morin.yd_by_date import YDbyDate
        obj = object.__new__(YDbyDate)
        class FakeCommon:
            def log_func(self, *args): pass
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        return obj

    def test_parses_tsv_response(self):
        obj = self._make_obj()
        tsv_text = "Date\tClicks\tCost\n2025-01-15\t100\t500.50\n2025-01-16\t200\t1000.00"

        class FakeResponse:
            text = tsv_text

        result = obj.tsv_to_dict(FakeResponse())
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['Date'] == '2025-01-15'
        assert result[0]['Clicks'] == 100
        assert result[0]['Cost'] == 500.50
        assert result[1]['Date'] == '2025-01-16'

    def test_single_row_tsv(self):
        obj = self._make_obj()
        tsv_text = "CampaignId\tImpressions\n12345\t999"

        class FakeResponse:
            text = tsv_text

        result = obj.tsv_to_dict(FakeResponse())
        assert len(result) == 1
        assert result[0]['CampaignId'] == 12345
        assert result[0]['Impressions'] == 999

    def test_empty_tsv_raises(self):
        obj = self._make_obj()
        tsv_text = ""

        class FakeResponse:
            text = tsv_text

        with pytest.raises(Exception):
            obj.tsv_to_dict(FakeResponse())


# =====================================================================
#  Live-тесты: получение кампаний
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetCampaigns:
    def test_returns_list(self):
        api = get_api_client()
        data = {"method": "get", "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name"]}}
        result = api._request('POST', '/json/v5/campaigns', json=data)
        assert 'result' in result
        assert 'Campaigns' in result['result']
        campaigns = result['result']['Campaigns']
        assert isinstance(campaigns, list)
        print(f"\nКампаний: {len(campaigns)}")

    def test_campaign_has_id_and_name(self):
        api = get_api_client()
        data = {"method": "get", "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name"]}}
        result = api._request('POST', '/json/v5/campaigns', json=data)
        campaigns = result['result']['Campaigns']
        if len(campaigns) == 0:
            pytest.skip("Нет кампаний для теста")
        c = campaigns[0]
        assert 'Id' in c
        assert 'Name' in c
        print(f"\nПервая кампания: Id={c['Id']}, Name={c['Name']}")


# =====================================================================
#  Live-тесты: получение отчёта (TSV)
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetReport:
    def test_report_returns_data(self):
        from morin.common import Common
        api = get_api_client()
        common = Common('', [''], '')
        report_name = common.shorten_text(str(YESTERDAY) + str(YESTERDAY) + 'test_report')
        extra_headers = {
            "processingMode": "auto", "returnMoneyInMicros": "false",
            "skipReportHeader": "true", "skipColumnHeader": "false", "skipReportSummary": "true"
        }
        report_dict = {
            "SelectionCriteria": {"DateFrom": YESTERDAY, "DateTo": YESTERDAY},
            "FieldNames": ["Date", "Clicks", "Cost", "Impressions"],
            "ReportName": report_name,
            "Page": {"Limit": 100},
            "ReportType": "CUSTOM_REPORT", "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV", "IncludeVAT": "YES", "IncludeDiscount": "NO"
        }
        data = {"params": report_dict}
        response = api._request_raw('POST', '/json/v5/reports', json=data, headers=extra_headers)
        status = response.status_code
        assert status in (200, 201, 202)
        if status == 200:
            assert len(response.text) > 0
            lines = response.text.strip().split('\n')
            assert len(lines) >= 1
            print(f"\nОтчёт получен, строк: {len(lines)}")
            print(f"Заголовок: {lines[0][:200]}")
        else:
            print(f"\nОтчёт принят на обработку, status={status}")


# =====================================================================
#  Live-тесты: получение объявлений
# =====================================================================

@needs_credentials
@needs_httpx
class TestGetAds:
    def _get_first_campaign_id(self):
        api = get_api_client()
        data = {"method": "get", "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name"]}}
        result = api._request('POST', '/json/v5/campaigns', json=data)
        campaigns = result['result']['Campaigns']
        if not campaigns:
            return None
        return campaigns[0]['Id']

    def test_returns_ads_list(self):
        campaign_id = self._get_first_campaign_id()
        if not campaign_id:
            pytest.skip("Нет кампаний для теста")
        time.sleep(5)
        api = get_api_client()
        body = {"method": "get",
                "params": {"SelectionCriteria": {"CampaignIds": [int(campaign_id)]},
                "FieldNames": ["CampaignId", "Id", "State", "Status"],
                "TextAdFieldNames": ["Title", "Title2", "Text", "Href"],
                "Page": {"Limit": 10, "Offset": 0}
                }}
        result = api._request('POST', '/json/v5/ads', json=body)
        assert 'result' in result
        ads_data = result['result']
        if ads_data and 'Ads' in ads_data:
            ads = ads_data['Ads']
            assert isinstance(ads, list)
            if len(ads) > 0:
                ad = ads[0]
                assert 'Id' in ad
                assert 'CampaignId' in ad
                print(f"\nОбъявлений: {len(ads)}, первое: Id={ad['Id']}")


# =====================================================================
#  Ручной запуск: python tests/test_yd.py
# =====================================================================
if __name__ == '__main__':
    DELAY_SECONDS = 60

    METHODS_TO_TEST = [
        'tsv_to_dict',
        'get_campaigns',
        'get_report',
        'get_ads',
    ]

    def run_tsv_to_dict():
        from morin.yd_by_date import YDbyDate
        class FakeCommon:
            def log_func(self, *args): pass
        obj = object.__new__(YDbyDate)
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        tsv_text = "Date\tClicks\tCost\n2025-01-15\t100\t500.50\n2025-01-16\t200\t1000.00"
        class FakeResponse:
            text = tsv_text
        result = obj.tsv_to_dict(FakeResponse())
        print(f"  rows: {len(result)}")
        print(f"  data: {result}")

    def run_get_campaigns():
        api = get_api_client()
        data = {"method": "get", "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name"]}}
        result = api._request('POST', '/json/v5/campaigns', json=data)
        campaigns = result['result']['Campaigns']
        print(f"  Кампаний: {len(campaigns)}")
        for c in campaigns[:5]:
            print(f"  Id={c['Id']}, Name={c['Name']}")
        return [c['Id'] for c in campaigns]

    def run_get_report():
        from morin.common import Common
        api = get_api_client()
        common = Common('', [''], '')
        report_name = common.shorten_text(str(YESTERDAY) + str(YESTERDAY) + 'test_manual')
        extra_headers = {
            "processingMode": "auto", "returnMoneyInMicros": "false",
            "skipReportHeader": "true", "skipColumnHeader": "false", "skipReportSummary": "true"
        }
        report_dict = {
            "SelectionCriteria": {"DateFrom": YESTERDAY, "DateTo": YESTERDAY},
            "FieldNames": ["Date", "Clicks", "Cost", "Impressions"],
            "ReportName": report_name,
            "Page": {"Limit": 100},
            "ReportType": "CUSTOM_REPORT", "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV", "IncludeVAT": "YES", "IncludeDiscount": "NO"
        }
        data = {"params": report_dict}
        response = api._request_raw('POST', '/json/v5/reports', json=data, headers=extra_headers)
        print(f"  status: {response.status_code}")
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            print(f"  Строк: {len(lines)}")
            for line in lines[:5]:
                print(f"  {line[:200]}")
        elif response.status_code == 201:
            print(f"  Отчёт принят на обработку, ожидание...")
            for i in range(30):
                time.sleep(10)
                response = api._request_raw('POST', '/json/v5/reports', json=data, headers=extra_headers)
                print(f"  Попытка {i+1}: status={response.status_code}")
                if response.status_code == 200:
                    lines = response.text.strip().split('\n')
                    print(f"  Строк: {len(lines)}")
                    for line in lines[:5]:
                        print(f"  {line[:200]}")
                    break

    def run_get_ads(campaign_ids):
        if not campaign_ids:
            print("  Нет кампаний — пропуск")
            return
        api = get_api_client()
        body = {"method": "get",
                "params": {"SelectionCriteria": {"CampaignIds": [int(campaign_ids[0])]},
                "FieldNames": ["CampaignId", "Id", "State", "Status"],
                "TextAdFieldNames": ["Title", "Title2", "Text", "Href"],
                "Page": {"Limit": 10, "Offset": 0}
                }}
        result = api._request('POST', '/json/v5/ads', json=body)
        ads_data = result.get('result', {})
        ads = ads_data.get('Ads', [])
        print(f"  Объявлений: {len(ads)}")
        for ad in ads[:5]:
            text_ad = ad.get('TextAd', {})
            print(f"  Id={ad['Id']}, Title={text_ad.get('Title','')}")

    if not YD_TOKEN or not YD_LOGIN:
        print("YD_TOKEN / YD_LOGIN не заполнены!")
        print("Запускаю только юнит-тесты...\n")
        METHODS_TO_TEST = ['tsv_to_dict']

    campaign_ids = None
    for method in METHODS_TO_TEST:
        print(f"\n{'='*60}")
        print(f"  {method}")
        print(f"{'='*60}")
        try:
            if method == 'tsv_to_dict':
                run_tsv_to_dict()
            elif method == 'get_campaigns':
                campaign_ids = run_get_campaigns()
            elif method == 'get_report':
                run_get_report()
            elif method == 'get_ads':
                run_get_ads(campaign_ids)
            print(f"  ✓ OK")
        except Exception as e:
            print(f"  ✗ ОШИБКА: {e}")
        if method != METHODS_TO_TEST[-1] and method != 'tsv_to_dict':
            print(f"\n  Пауза {DELAY_SECONDS} сек...")
            time.sleep(DELAY_SECONDS)
