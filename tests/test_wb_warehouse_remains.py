import sys
import os
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin.base_client import BaseMarketplaceClient

# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
WB_TOKEN = ''
DUMP_JSON = False   # True — сохранить сырой ответ в wb_warehouse_remains.json
# =================================

BASE = 'https://seller-analytics-api.wildberries.ru'


def create_task(api):
    url = f'{BASE}/api/v1/warehouse_remains'
    params = {
        'groupByBrand': 'true',
        'groupBySubject': 'true',
        'groupBySa': 'true',
        'groupByNm': 'true',
        'groupByBarcode': 'true',
        'groupBySize': 'true',
    }
    print(f'GET {url}')
    print(f'params: {params}')
    data = api._request('GET', url, params=params)
    task_id = data.get('data', {}).get('taskId')
    print(f'taskId: {task_id}')
    return task_id


def poll_status(api, task_id):
    url = f'{BASE}/api/v1/warehouse_remains/tasks/{task_id}/status'
    print(f'\nОпрос статуса {url}')
    for i in range(60):
        time.sleep(10)
        try:
            data = api._request('GET', url)
            status = data.get('data', {}).get('status')
            print(f'  Попытка {i+1}: {status}')
            if status == 'done':
                return True
        except Exception as e:
            print(f'  Попытка {i+1}: ошибка {e}')
    return False


def download(api, task_id):
    url = f'{BASE}/api/v1/warehouse_remains/tasks/{task_id}/download'
    print(f'\nGET {url}')
    response = api._request_raw('GET', url)
    print(f'status={response.status_code}, content_length={len(response.content)}')
    if not response.content:
        return []
    return response.json()


def main():
    if not WB_TOKEN:
        print('Заполните WB_TOKEN в начале файла!')
        sys.exit(1)

    api = BaseMarketplaceClient(
        base_url='',
        headers={'Authorization': WB_TOKEN, 'Content-Type': 'application/json'}
    )

    print('=' * 70)
    print('ШАГ 1. Создание задания')
    print('=' * 70)
    task_id = create_task(api)
    if not task_id:
        print('Не получили taskId')
        sys.exit(1)

    print('\n' + '=' * 70)
    print('ШАГ 2. Ожидание готовности')
    print('=' * 70)
    if not poll_status(api, task_id):
        print('Отчёт не готов за отведённое время')
        sys.exit(1)

    print('\n' + '=' * 70)
    print('ШАГ 3. Скачивание')
    print('=' * 70)
    raw = download(api, task_id)
    if DUMP_JSON:
        with open('wb_warehouse_remains.json', 'w', encoding='utf-8') as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        print('Сохранено: wb_warehouse_remains.json')

    if not isinstance(raw, list):
        print(f'Ответ не массив: тип {type(raw)}')
        print(f'raw={str(raw)[:500]}')
        sys.exit(1)

    print(f'\nВсего товаров в ответе: {len(raw)}')

    if raw:
        print('\nПример первой позиции (сырая):')
        print(json.dumps(raw[0], ensure_ascii=False, indent=2))

    # Расплющивание — как в коннекторе
    result = []
    for item in raw:
        wh_list = item.get('warehouses') or []
        base = {k: v for k, v in item.items() if k != 'warehouses'}
        if not wh_list:
            result.append(base)
            continue
        for wh in wh_list:
            row = dict(base)
            for k, v in wh.items():
                row[k] = v
            result.append(row)

    print('\n' + '=' * 70)
    print(f'ИТОГ: строк после расплющивания {len(result)}')
    if result:
        print('\nПоля первой строки:')
        for k, v in result[0].items():
            print(f'  {k:25} = {v}')

        # Топ складов
        wh_counter = {}
        for r in result:
            wh = r.get('warehouseName', 'НЕТ')
            wh_counter[wh] = wh_counter.get(wh, 0) + 1
        print('\nПо складам (топ-10 по числу позиций):')
        for name, cnt in sorted(wh_counter.items(), key=lambda x: -x[1])[:10]:
            print(f'  {name:30} {cnt}')


if __name__ == '__main__':
    main()
