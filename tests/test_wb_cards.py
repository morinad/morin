import sys
import os
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin.base_client import BaseMarketplaceClient

# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
WB_TOKEN = ''       # Content API токен (Personal или Service)
MAX_CARDS = 300     # сколько карточек максимум собрать (для теста, чтобы не тянуть всё)
DUMP_JSON = False   # True — сохранить raw JSON первой страницы в wb_cards_first_page.json
# =================================

URL = 'https://content-api.wildberries.ru/content/v2/get/cards/list'


def fetch_page(api, cursor):
    payload = {
        'settings': {
            'sort': {'ascending': True},
            'filter': {'withPhoto': -1},
            'cursor': cursor
        }
    }
    return api._request('POST', URL, json=payload)


def main():
    if not WB_TOKEN:
        print('Заполните WB_TOKEN в начале файла!')
        sys.exit(1)

    api = BaseMarketplaceClient(
        base_url='',
        headers={'Authorization': WB_TOKEN, 'Content-Type': 'application/json'}
    )

    print(f'POST {URL}')
    print(f'Максимум карточек за прогон: {MAX_CARDS}')
    print('=' * 70)

    limit = 100
    cursor = {'limit': limit}
    all_cards = []
    page_num = 0
    total_type_counter = {}

    while len(all_cards) < MAX_CARDS:
        page_num += 1
        print(f'\n--- Страница {page_num} ---')
        print(f'Отправлен cursor: {json.dumps(cursor, ensure_ascii=False)}')
        try:
            data = fetch_page(api, cursor)
        except Exception as e:
            print(f'Ошибка запроса: {e}')
            if hasattr(e, 'response') and e.response is not None:
                print(f'Status: {e.response.status_code}')
                try:
                    print(f'Body: {e.response.text[:1000]}')
                except Exception:
                    pass
            break

        cards = data.get('cards', []) or []
        cursor_info = data.get('cursor', {}) or {}
        total = cursor_info.get('total', 0)

        print(f'В ответе: {len(cards)} карточек, cursor.total={total}')
        print(f'cursor из ответа: updatedAt={cursor_info.get("updatedAt")}, nmID={cursor_info.get("nmID")}')

        if page_num == 1 and DUMP_JSON:
            with open('wb_cards_first_page.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print('Сохранено: wb_cards_first_page.json')

        if page_num == 1 and cards:
            print(f'\nПример полей первой карточки:')
            first = cards[0]
            for key in list(first.keys())[:20]:
                val = first[key]
                if isinstance(val, (list, dict)):
                    val = f'{type(val).__name__} с {len(val)} эл.'
                print(f'  {key:20} = {val}')

        for c in cards:
            subj = c.get('subjectName', 'НЕТ_ПРЕДМЕТА')
            total_type_counter[subj] = total_type_counter.get(subj, 0) + 1
            all_cards.append(c)

        if total < limit:
            print(f'\nПоследняя страница (total {total} < limit {limit}).')
            break

        next_updated = cursor_info.get('updatedAt')
        next_nm = cursor_info.get('nmID')
        if not next_updated or not next_nm:
            print('\nОстановка: нет updatedAt/nmID в курсоре ответа.')
            break

        cursor = {'limit': limit, 'updatedAt': next_updated, 'nmID': next_nm}
        time.sleep(1)

    print('\n' + '=' * 70)
    print(f'ИТОГО: {len(all_cards)} карточек за {page_num} страниц(у)')
    if total_type_counter:
        print('\nРаспределение по subjectName (топ-15):')
        top = sorted(total_type_counter.items(), key=lambda x: -x[1])[:15]
        for name, cnt in top:
            print(f'  {name:40} {cnt:4}')


if __name__ == '__main__':
    main()
