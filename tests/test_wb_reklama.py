import sys
import os
import time
import requests
import pytest
import pandas as pd
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
WB_TOKEN = ''
# =================================

credentials_filled = bool(WB_TOKEN)
skip_reason = 'WB_TOKEN не заполнен — вставьте свой токен в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')


@needs_credentials
class TestPromotionCount:
    def test_returns_200(self):
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        assert response.status_code == 200

    def test_response_has_adverts(self):
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        result = response.json()
        assert 'adverts' in result
        assert 'all' in result

    def test_advert_list_has_ids(self):
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        result = response.json()
        advert_ids = []
        for advert in result['adverts']:
            assert 'advert_list' in advert
            for item in advert['advert_list']:
                assert 'advertId' in item
                advert_ids.append(item['advertId'])
        assert len(advert_ids) > 0
        print(f"\nНайдено кампаний: {len(advert_ids)}")
        print(f"Первые 5 ID: {advert_ids[:5]}")


@needs_credentials
class TestAdvertsV2:
    def _get_campaign_ids(self):
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        result = response.json()
        ids = []
        for advert in result['adverts']:
            for item in advert['advert_list']:
                ids.append(item['advertId'])
        return ids[:5]

    def test_returns_200(self):
        ids = self._get_campaign_ids()
        if not ids:
            pytest.skip("Нет кампаний для теста")
        time.sleep(10)
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
        ids_str = ','.join(str(c) for c in ids)
        response = requests.get(url, params={'ids': ids_str}, headers=headers)
        assert response.status_code == 200

    def test_response_structure(self):
        ids = self._get_campaign_ids()
        if not ids:
            pytest.skip("Нет кампаний для теста")
        time.sleep(10)
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
        ids_str = ','.join(str(c) for c in ids)
        response = requests.get(url, params={'ids': ids_str}, headers=headers)
        result = response.json()
        assert 'adverts' in result
        campaigns = result['adverts']
        assert len(campaigns) > 0
        c = campaigns[0]
        assert 'id' in c
        assert 'status' in c
        assert 'settings' in c
        assert 'name' in c['settings']
        assert 'timestamps' in c
        ts = c['timestamps']
        assert 'created' in ts
        assert 'updated' in ts
        print(f"\nПервая кампания: id={c['id']}, status={c['status']}, name={c['settings']['name']}")
        print(f"timestamps: {ts}")


@needs_credentials
class TestFullstatsV3:
    def _get_campaign_ids(self):
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        result = response.json()
        ids = []
        for advert in result['adverts']:
            if advert.get('status') in (9, 11):
                for item in advert['advert_list']:
                    ids.append(item['advertId'])
        return ids[:3]

    def test_returns_200(self):
        ids = self._get_campaign_ids()
        if not ids:
            pytest.skip("Нет активных кампаний для теста")
        time.sleep(10)
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
        ids_str = ','.join(str(c) for c in ids)
        params = {'ids': ids_str, 'beginDate': WEEK_AGO, 'endDate': YESTERDAY}
        response = requests.get(url, headers=headers, params=params)
        assert response.status_code == 200

    def test_response_structure(self):
        ids = self._get_campaign_ids()
        if not ids:
            pytest.skip("Нет активных кампаний для теста")
        time.sleep(10)
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
        ids_str = ','.join(str(c) for c in ids)
        params = {'ids': ids_str, 'beginDate': WEEK_AGO, 'endDate': YESTERDAY}
        response = requests.get(url, headers=headers, params=params)
        result = response.json()
        assert isinstance(result, list)
        if len(result) > 0:
            advert = result[0]
            assert 'advertId' in advert
            assert 'days' in advert
            if len(advert['days']) > 0:
                day = advert['days'][0]
                assert 'date' in day
                assert 'apps' in day
                if len(day['apps']) > 0:
                    app = day['apps'][0]
                    assert 'appType' in app
                    assert 'nms' in app
                    if len(app['nms']) > 0:
                        nm = app['nms'][0]
                        assert 'nmId' in nm
                        assert 'views' in nm
                        assert 'clicks' in nm
                        assert 'sum' in nm
                        assert 'name' in nm
                        print(f"\nnmId={nm['nmId']}, views={nm['views']}, clicks={nm['clicks']}, sum={nm['sum']}")
            print(f"advertId={advert['advertId']}, days={len(advert['days'])}")


class TestExtractDf:
    def test_parses_v3_response(self):
        from morin.common import Common
        from morin.wb_reklama import WBreklama

        mock_response = [
            {
                "advertId": 123,
                "boosterStats": [
                    {"date": "2025-01-01", "nm": 456, "avg_position": 5}
                ],
                "days": [
                    {
                        "date": "2025-01-01T00:00:00Z",
                        "apps": [
                            {
                                "appType": 1,
                                "nms": [
                                    {
                                        "nmId": 789,
                                        "name": "Test Product",
                                        "views": 100,
                                        "clicks": 10,
                                        "sum": 50.5,
                                        "atbs": 3,
                                        "orders": 2,
                                        "shks": 2,
                                        "sum_price": 1000.0
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

        class FakeCommon:
            def __init__(self): pass
            def log_func(self, *args): pass

        class FakeClient:
            def __init__(self): pass

        obj = object.__new__(WBreklama)
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        obj.now = datetime.now()

        df, booster_df, out_json, out_booster_json = obj.extract_df(mock_response)

        assert len(df) == 1
        assert df.iloc[0]['advertId'] == 123
        assert df.iloc[0]['nmId'] == 789
        assert df.iloc[0]['views'] == 100
        assert df.iloc[0]['clicks'] == 10
        assert df.iloc[0]['sum'] == 50.5
        assert df.iloc[0]['name'] == 'Test Product'

        assert len(booster_df) == 1
        assert booster_df.iloc[0]['nm'] == 456
        assert booster_df.iloc[0]['avgPosition'] == 5

        assert len(out_json) == 1
        assert out_json[0]['advertId'] == 123
        assert out_json[0]['nmId'] == 789

        assert len(out_booster_json) == 1
        assert out_booster_json[0]['nm'] == 456


# =====================================================================
#  Ручной запуск: python tests/test_wb_reklama.py
# =====================================================================
if __name__ == '__main__':
    DELAY_SECONDS = 60

    METHODS_TO_TEST = [
        'promotion_count',
        'adverts_v2',
        'fullstats_v3',
        'extract_df',
    ]

    def run_promotion_count():
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        print(f"  status: {response.status_code}")
        result = response.json()
        ids = []
        for advert in result.get('adverts', []):
            for item in advert.get('advert_list', []):
                ids.append(item['advertId'])
        print(f"  Кампаний: {len(ids)}")
        print(f"  Первые 10: {ids[:10]}")
        return ids

    def run_adverts_v2(campaign_ids):
        if not campaign_ids:
            print("  Нет кампаний — пропуск")
            return
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
        ids_str = ','.join(str(c) for c in campaign_ids[:5])
        response = requests.get(url, params={'ids': ids_str}, headers=headers)
        print(f"  status: {response.status_code}")
        result = response.json()
        campaigns = result.get('adverts', [])
        print(f"  Получено кампаний: {len(campaigns)}")
        for c in campaigns[:3]:
            ts = c.get('timestamps', {})
            print(f"  id={c['id']}, status={c['status']}, name={c.get('settings',{}).get('name','')}")
            print(f"    created={ts.get('created','')}, updated={ts.get('updated','')}, deleted={ts.get('deleted','')}")

    def run_fullstats_v3(campaign_ids):
        if not campaign_ids:
            print("  Нет кампаний — пропуск")
            return
        headers = {"Authorization": WB_TOKEN}
        url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
        ids_str = ','.join(str(c) for c in campaign_ids[:3])
        params = {'ids': ids_str, 'beginDate': WEEK_AGO, 'endDate': YESTERDAY}
        response = requests.get(url, headers=headers, params=params)
        print(f"  status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"  Кампаний в ответе: {len(result)}")
            for advert in result[:2]:
                print(f"  advertId={advert['advertId']}, days={len(advert.get('days', []))}")
                for day in advert.get('days', [])[:1]:
                    for app in day.get('apps', [])[:1]:
                        print(f"    appType={app['appType']}, nms={len(app.get('nms', []))}")
                        for nm in app.get('nms', [])[:2]:
                            print(f"      nmId={nm['nmId']}, views={nm['views']}, clicks={nm['clicks']}, sum={nm['sum']}")
        else:
            print(f"  Ошибка: {response.text[:500]}")

    def run_extract_df():
        from morin.wb_reklama import WBreklama
        class FakeCommon:
            def log_func(self, *args): pass
        obj = object.__new__(WBreklama)
        obj.common = FakeCommon()
        obj.bot_token = ''
        obj.chat_list = ''
        obj.add_name = 'test'
        obj.now = datetime.now()
        mock = [{
            "advertId": 123,
            "boosterStats": [{"date": "2025-01-01", "nm": 456, "avg_position": 5}],
            "days": [{"date": "2025-01-01T00:00:00Z", "apps": [
                {"appType": 1, "nms": [
                    {"nmId": 789, "name": "Test", "views": 100, "clicks": 10,
                     "sum": 50.5, "atbs": 3, "orders": 2, "shks": 2, "sum_price": 1000.0}
                ]}
            ]}]
        }]
        df, booster_df, out_json, out_booster_json = obj.extract_df(mock)
        print(f"  data rows: {len(df)}, booster rows: {len(booster_df)}")
        print(f"  raw data dicts: {len(out_json)}, raw booster dicts: {len(out_booster_json)}")
        print(f"  columns: {list(df.columns)}")
        print(f"  data: {df.to_dict('records')}")

    RUNNERS = {
        'promotion_count': lambda: run_promotion_count(),
        'adverts_v2': None,
        'fullstats_v3': None,
        'extract_df': lambda: run_extract_df(),
    }

    if not WB_TOKEN:
        print("WB_TOKEN не заполнен!")
        sys.exit(1)

    campaign_ids = None
    for method in METHODS_TO_TEST:
        print(f"\n{'='*60}")
        print(f"  {method}")
        print(f"{'='*60}")
        try:
            if method == 'promotion_count':
                campaign_ids = run_promotion_count()
            elif method == 'adverts_v2':
                run_adverts_v2(campaign_ids)
            elif method == 'fullstats_v3':
                run_fullstats_v3(campaign_ids)
            elif method == 'extract_df':
                run_extract_df()
            print(f"  OK")
        except Exception as e:
            print(f"  ОШИБКА: {e}")
        if method != METHODS_TO_TEST[-1] and method != 'extract_df':
            print(f"\n  Пауза {DELAY_SECONDS} сек...")
            time.sleep(DELAY_SECONDS)
