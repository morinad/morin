import sys
import os
import time
import json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin.base_client import BaseMarketplaceClient

# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
OZON_ADS_CLIENT_ID = ''
OZON_ADS_CLIENT_SECRET = ''
CAMPAIGN_ID = ''
DATE = ''
# =================================

BASE_URL = 'https://api-performance.ozon.ru'


def get_token(api):
    payload = {"client_id": OZON_ADS_CLIENT_ID, "client_secret": OZON_ADS_CLIENT_SECRET, "grant_type": "client_credentials"}
    result = api._request('POST', '/api/client/token', json=payload)
    token = result['access_token']
    api.client.headers['Authorization'] = f'Bearer {token}'
    print(f"Токен получен, expires_in: {result.get('expires_in')}")
    return token


def get_campaign_info(api, campaign_id):
    result = api._request('GET', '/api/client/campaign')
    campaigns = result['list']
    for c in campaigns:
        if str(c['id']) == str(campaign_id):
            print(f"\nИнфо о кампании:")
            for key in ['id', 'title', 'state', 'advObjectType', 'createdAt', 'fromDate', 'toDate', 'dailyBudget']:
                print(f"  {key:20s} {c.get(key, '')}")
            return c
    print(f"\nКампания {campaign_id} не найдена!")
    return None


def try_request(api, label, url, payload):
    print(f"\n{'─' * 60}")
    print(f"Попытка: {label}")
    print(f"URL:     POST {BASE_URL}{url}")
    print(f"Payload: {json.dumps(payload, ensure_ascii=False)}")
    try:
        result = api._request('POST', url, json=payload)
        print(f"УСПЕХ! Ответ: {json.dumps(result, ensure_ascii=False)[:500]}")
        return result
    except Exception as e:
        status = getattr(e, 'response', None)
        if status is not None:
            print(f"ОШИБКА: {status.status_code}")
            print(f"Body:   {status.text[:1000]}")
        else:
            print(f"ОШИБКА: {e}")
        return None


def wait_for_report(api, uuid, max_attempts=30, interval=10):
    print(f"\nОжидание отчёта UUID={uuid}...")
    for k in range(max_attempts):
        time.sleep(interval)
        try:
            status = api._request('GET', f'/api/client/statistics/{uuid}')
            state = status.get('state', '')
            print(f"  [{k+1}] state={state}")
            if state == 'OK':
                return True
        except Exception as e:
            print(f"  [{k+1}] ошибка: {e}")
    return False


def download_report(api, uuid):
    response = api._request_raw('GET', '/api/client/statistics/report', params={'UUID': uuid})
    print(f"\nСкачан отчёт: status={response.status_code}, bytes={len(response.content)}")
    lines = response.text.splitlines()
    print(f"Строк: {len(lines)}")
    for i, line in enumerate(lines[:10]):
        print(f"  [{i}] {line[:200]}")
    return response.text


if __name__ == '__main__':
    print(f"Тест SEARCH_PROMO кампании {CAMPAIGN_ID} за {DATE}")
    print("=" * 60)

    api = BaseMarketplaceClient(
        base_url=BASE_URL,
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
    )

    get_token(api)
    campaign_info = get_campaign_info(api, CAMPAIGN_ID)
    time.sleep(3)

    cid = CAMPAIGN_ID

    date_iso_from = f"{DATE}T00:00:00Z"
    date_iso_to = f"{DATE}T23:59:59Z"

    # 1. from/to ISO 8601 (CSV)
    r1 = try_request(api, "CSV from/to ISO 8601", '/api/client/statistics', {
        "campaigns": [str(cid)],
        "from": date_iso_from,
        "to": date_iso_to,
        "groupBy": "NO_GROUP_BY"
    })
    time.sleep(3)

    # 2. from/to ISO 8601 (JSON)
    r2 = try_request(api, "JSON from/to ISO 8601", '/api/client/statistics/json', {
        "campaigns": [str(cid)],
        "from": date_iso_from,
        "to": date_iso_to,
        "groupBy": "NO_GROUP_BY"
    })
    time.sleep(3)

    # 3. from/to ISO + groupBy=DATE (CSV)
    r3 = try_request(api, "CSV from/to ISO + groupBy=DATE", '/api/client/statistics', {
        "campaigns": [str(cid)],
        "from": date_iso_from,
        "to": date_iso_to,
        "groupBy": "DATE"
    })
    time.sleep(3)

    # 4. Все 4 поля дат (from/to + dateFrom/dateTo)
    r4 = try_request(api, "CSV все 4 поля дат", '/api/client/statistics', {
        "campaigns": [str(cid)],
        "from": date_iso_from,
        "to": date_iso_to,
        "dateFrom": DATE,
        "dateTo": DATE,
        "groupBy": "NO_GROUP_BY"
    })
    time.sleep(3)

    # 5. from/to ISO + JSON + groupBy=DATE
    r5 = try_request(api, "JSON from/to ISO + groupBy=DATE", '/api/client/statistics/json', {
        "campaigns": [str(cid)],
        "from": date_iso_from,
        "to": date_iso_to,
        "groupBy": "DATE"
    })
    time.sleep(3)

    # Если какой-то запрос вернул UUID — скачиваем
    for label, result in [("r1", r1), ("r2", r2), ("r3", r3), ("r4", r4), ("r5", r5)]:
        if result and result.get('UUID'):
            print(f"\n{'=' * 60}")
            print(f"{label} вернул UUID — скачиваем отчёт")
            uuid = result['UUID']
            if wait_for_report(api, uuid):
                download_report(api, uuid)
            else:
                print("Отчёт не готов!")
            break
    else:
        print(f"\n{'=' * 60}")
        print("Ни один вариант не вернул UUID. SEARCH_PROMO не поддерживается через /api/client/statistics.")
