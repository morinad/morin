import sys
import os
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin.base_client import BaseMarketplaceClient

# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
WB_TOKEN = ''
MAX_CARDS = 2000     # чтобы тест не занимал часы — можно ограничить
MAX_WAREHOUSES = 5  # взять только N складов для теста (None — все)
DUMP_JSON = False   # True — сохранить первый батч ответа в wb_stocks_wh_sample.json
# =================================

URL_WAREHOUSES = 'https://marketplace-api.wildberries.ru/api/v3/warehouses'
URL_CARDS = 'https://content-api.wildberries.ru/content/v2/get/cards/list'


def get_warehouses(api):
    print(f'GET {URL_WAREHOUSES}')
    data = api._request('GET', URL_WAREHOUSES)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get('data') or data.get('warehouses') or []
    return []


def get_all_chrt_ids(api, max_cards=None):
    limit = 100
    cursor = {'limit': limit}
    chrt_ids = set()
    total_cards = 0
    page = 0
    while True:
        page += 1
        payload = {
            'settings': {
                'sort': {'ascending': True},
                'filter': {'withPhoto': -1},
                'cursor': cursor
            }
        }
        data = api._request('POST', URL_CARDS, json=payload)
        cards = data.get('cards', []) or []
        cursor_info = data.get('cursor', {}) or {}
        for card in cards:
            for size in card.get('sizes', []) or []:
                chrt_id = size.get('chrtID')
                if chrt_id:
                    chrt_ids.add(chrt_id)
            total_cards += 1
        total = cursor_info.get('total', 0)
        print(f'  Страница {page}: карточек {len(cards)}, chrtIds уникальных: {len(chrt_ids)}, total_cards={total_cards}')
        if max_cards and total_cards >= max_cards:
            print(f'  Достигнут лимит MAX_CARDS={max_cards}')
            break
        if total < limit:
            break
        next_updated = cursor_info.get('updatedAt')
        next_nm = cursor_info.get('nmID')
        if not next_updated or not next_nm:
            break
        cursor = {'limit': limit, 'updatedAt': next_updated, 'nmID': next_nm}
        time.sleep(1)
    return list(chrt_ids)


def main():
    if not WB_TOKEN:
        print('Заполните WB_TOKEN в начале файла!')
        sys.exit(1)

    api = BaseMarketplaceClient(
        base_url='',
        headers={'Authorization': WB_TOKEN, 'Content-Type': 'application/json'}
    )

    print('=' * 70)
    print('ШАГ 1. Список складов')
    print('=' * 70)
    warehouses = get_warehouses(api)
    print(f'Найдено складов: {len(warehouses)}')
    for w in warehouses:
        print(f"  id={w.get('id')}, officeId={w.get('officeId')}, name={w.get('name')}, "
              f"deliveryType={w.get('deliveryType')}, cargoType={w.get('cargoType')}")
    if MAX_WAREHOUSES:
        warehouses = warehouses[:MAX_WAREHOUSES]
        print(f'\n(для теста берём первые {len(warehouses)})')

    print('\n' + '=' * 70)
    print('ШАГ 2. Сбор chrtIds из карточек')
    print('=' * 70)
    chrt_ids = get_all_chrt_ids(api, max_cards=MAX_CARDS)
    print(f'\nВсего chrtIds: {len(chrt_ids)}')
    if not chrt_ids:
        print('chrtIds не собраны, дальше идти нет смысла.')
        sys.exit(1)

    print('\n' + '=' * 70)
    print('ШАГ 3. Остатки по складам')
    print('=' * 70)
    total_positions = 0
    total_amount = 0
    for wh in warehouses:
        wh_id = wh.get('id')
        wh_name = wh.get('name', '')
        print(f'\nСклад id={wh_id} name="{wh_name}"')
        url = f'https://marketplace-api.wildberries.ru/api/v3/stocks/{wh_id}'
        wh_positions = 0
        wh_amount = 0
        for i in range(0, len(chrt_ids), 1000):
            batch = chrt_ids[i:i + 1000]
            try:
                data = api._request('POST', url, json={'chrtIds': batch})
            except Exception as e:
                print(f'  Батч {i}: ОШИБКА {e}')
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        print(f'    Body: {e.response.text[:500]}')
                    except Exception:
                        pass
                continue

            if DUMP_JSON and wh_positions == 0:
                with open('wb_stocks_wh_sample.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print('  → сохранил wb_stocks_wh_sample.json')

            stocks = data.get('stocks', []) or []
            for s in stocks:
                if s.get('amount', 0) > 0:
                    wh_positions += 1
                    wh_amount += s.get('amount', 0)
            print(f'  Батч {i}-{i + len(batch)}: получено {len(stocks)} строк')
            time.sleep(0.5)
        print(f'  Итого по складу: позиций с остатком={wh_positions}, суммарное кол-во={wh_amount}')
        total_positions += wh_positions
        total_amount += wh_amount

    print('\n' + '=' * 70)
    print(f'ФИНАЛЬНЫЙ ИТОГ: {total_positions} позиций с остатком, кол-во {total_amount} шт.')


if __name__ == '__main__':
    main()
