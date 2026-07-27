import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin.base_client import BaseMarketplaceClient

# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
WB_TOKEN = ''       # Marketplace API токен
DUMP_JSON = False   # True — сохранить raw JSON в wb_warehouses.json
# =================================

URL = 'https://marketplace-api.wildberries.ru/api/v3/warehouses'

CARGO_TYPES = {
    1: 'МГТ (малогабаритный)',
    2: 'СГТ (сверхгабаритный)',
    3: 'КГТ+ (крупногабаритный)',
}
DELIVERY_TYPES = {
    1: 'FBS (доставка на склад WB)',
    2: 'DBS (доставка силами продавца)',
    3: 'DBW (доставка курьером WB)',
    5: 'C&C (самовывоз)',
    6: 'EDBS (экспресс силами продавца)',
}


def main():
    if not WB_TOKEN:
        print('Заполните WB_TOKEN в начале файла!')
        sys.exit(1)

    api = BaseMarketplaceClient(
        base_url='',
        headers={'Authorization': WB_TOKEN, 'Content-Type': 'application/json'}
    )

    print(f'GET {URL}')
    print('=' * 70)

    try:
        data = api._request('GET', URL)
    except Exception as e:
        print(f'Ошибка: {e}')
        if hasattr(e, 'response') and e.response is not None:
            print(f'Status: {e.response.status_code}')
            try:
                print(f'Body: {e.response.text[:1000]}')
            except Exception:
                pass
        sys.exit(1)

    if isinstance(data, list):
        warehouses = data
    elif isinstance(data, dict):
        warehouses = data.get('data') or data.get('warehouses') or []
    else:
        warehouses = []

    print(f'\nВсего складов: {len(warehouses)}\n')

    if DUMP_JSON:
        with open('wb_warehouses.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print('Сохранено: wb_warehouses.json\n')

    for i, w in enumerate(warehouses, 1):
        print(f'--- Склад {i} ---')
        print(f"  id (продавца):    {w.get('id')}")
        print(f"  officeId (WB):    {w.get('officeId')}")
        print(f"  name:             {w.get('name')}")
        cargo = w.get('cargoType')
        print(f"  cargoType:        {cargo} — {CARGO_TYPES.get(cargo, 'неизвестный тип')}")
        delivery = w.get('deliveryType')
        print(f"  deliveryType:     {delivery} — {DELIVERY_TYPES.get(delivery, 'неизвестный тип')}")
        print(f"  isDeleting:       {w.get('isDeleting')}")
        print(f"  isProcessing:     {w.get('isProcessing')}")
        print()

    if warehouses:
        print('=' * 70)
        cnt_delivery = {}
        cnt_cargo = {}
        for w in warehouses:
            d = DELIVERY_TYPES.get(w.get('deliveryType'), f"type {w.get('deliveryType')}")
            c = CARGO_TYPES.get(w.get('cargoType'), f"type {w.get('cargoType')}")
            cnt_delivery[d] = cnt_delivery.get(d, 0) + 1
            cnt_cargo[c] = cnt_cargo.get(c, 0) + 1
        print('\nПо типу доставки:')
        for k, v in sorted(cnt_delivery.items(), key=lambda x: -x[1]):
            print(f"  {k:45} {v}")
        print('\nПо типу товара:')
        for k, v in sorted(cnt_cargo.items(), key=lambda x: -x[1]):
            print(f"  {k:45} {v}")


if __name__ == '__main__':
    main()
