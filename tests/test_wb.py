import sys
import os
import time
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from morin.wb_by_date import WBbyDate
    from morin.base_client import BaseMarketplaceClient
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

needs_httpx = pytest.mark.skipif(not HAS_HTTPX, reason='httpx не установлен — используйте старую версию без httpx')


# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
WB_TOKEN = ''
# =================================

credentials_filled = bool(WB_TOKEN)
skip_reason = 'WB_TOKEN не заполнен — вставьте свой токен в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')


@pytest.fixture
def wb():
    instance = WBbyDate(token=WB_TOKEN, add_name='test', start=START_DATE)
    yield instance
    instance.api.close()


# =====================================================================
#  wb_realized
# =====================================================================

@needs_httpx
class TestWbRealizedInit:
    def test_api_is_base_marketplace_client(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert isinstance(instance.api, BaseMarketplaceClient)

    def test_headers_contain_authorization(self):
        instance = WBbyDate(token='my_token', add_name='test', start='2025-01-01')
        headers = dict(instance.api.client.headers)
        assert headers.get('authorization') == 'my_token'

    def test_err429_default_false(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert instance.err429 is False

    def test_source_dict_has_realized(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'realized' in instance.source_dict
        assert instance.source_dict['realized']['func_name'] == instance.get_realized


@needs_credentials
@needs_httpx
class TestGetRealizedLive:
    def test_returns_list(self, wb):
        result = wb.get_realized(WEEK_AGO)
        assert not isinstance(result, str), f'get_realized вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_realized(WEEK_AGO)
        assert wb.err429 is False


# =====================================================================
#  wb_orders
# =====================================================================

@needs_httpx
class TestWbOrdersInit:
    def test_source_dict_has_orders(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'orders' in instance.source_dict
        assert instance.source_dict['orders']['func_name'] == instance.get_orders


@needs_credentials
@needs_httpx
class TestGetOrdersLive:
    def test_returns_list(self, wb):
        result = wb.get_orders(YESTERDAY)
        assert not isinstance(result, str), f'get_orders вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_orders(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_sbor_orders
# =====================================================================

@needs_httpx
class TestWbSborOrdersInit:
    def test_source_dict_has_sbor_orders(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'sbor_orders' in instance.source_dict
        assert instance.source_dict['sbor_orders']['func_name'] == instance.get_sbor


@needs_credentials
@needs_httpx
class TestGetSborLive:
    def test_returns_list(self, wb):
        result = wb.get_sbor(YESTERDAY)
        assert not isinstance(result, str), f'get_sbor вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_sbor(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_sbor_status
# =====================================================================

@needs_httpx
class TestWbSborStatusInit:
    def test_source_dict_has_sbor_status(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'sbor_status' in instance.source_dict
        assert instance.source_dict['sbor_status']['func_name'] == instance.get_sbor_status


@needs_credentials
@needs_httpx
class TestGetSborStatusLive:
    def test_returns_list(self, wb):
        result = wb.get_sbor_status(YESTERDAY)
        assert not isinstance(result, str), f'get_sbor_status вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_sbor_status(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_incomes
# =====================================================================

@needs_httpx
class TestWbIncomesInit:
    def test_source_dict_has_incomes(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'incomes' in instance.source_dict
        assert instance.source_dict['incomes']['func_name'] == instance.get_incomes


@needs_credentials
@needs_httpx
class TestGetIncomesLive:
    def test_returns_list(self, wb):
        result = wb.get_incomes()
        assert not isinstance(result, str), f'get_incomes вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_incomes()
        assert wb.err429 is False


# =====================================================================
#  wb_excise
# =====================================================================

@needs_httpx
class TestWbExciseInit:
    def test_source_dict_has_excise(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'excise' in instance.source_dict
        assert instance.source_dict['excise']['func_name'] == instance.get_excise


@needs_credentials
@needs_httpx
class TestGetExciseLive:
    def test_returns_list(self, wb):
        result = wb.get_excise(YESTERDAY)
        assert not isinstance(result, str), f'get_excise вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_excise(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_sales
# =====================================================================

@needs_httpx
class TestWbSalesInit:
    def test_source_dict_has_sales(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'sales' in instance.source_dict
        assert instance.source_dict['sales']['func_name'] == instance.get_sales


@needs_credentials
@needs_httpx
class TestGetSalesLive:
    def test_returns_list(self, wb):
        result = wb.get_sales(YESTERDAY)
        assert not isinstance(result, str), f'get_sales вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_sales(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_orders_changes
# =====================================================================

@needs_httpx
class TestWbOrdersChangesInit:
    def test_source_dict_has_orders_changes(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'orders_changes' in instance.source_dict
        assert instance.source_dict['orders_changes']['func_name'] == instance.get_orders_changes


@needs_credentials
@needs_httpx
class TestGetOrdersChangesLive:
    def test_returns_list(self, wb):
        result = wb.get_orders_changes(YESTERDAY)
        assert not isinstance(result, str), f'get_orders_changes вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_orders_changes(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_sales_changes
# =====================================================================

@needs_httpx
class TestWbSalesChangesInit:
    def test_source_dict_has_sales_changes(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'sales_changes' in instance.source_dict
        assert instance.source_dict['sales_changes']['func_name'] == instance.get_sales_changes


@needs_credentials
@needs_httpx
class TestGetSalesChangesLive:
    def test_returns_list(self, wb):
        result = wb.get_sales_changes(YESTERDAY)
        assert not isinstance(result, str), f'get_sales_changes вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_sales_changes(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_stocks  (stocks + stocks_history — одна функция)
# =====================================================================

@needs_httpx
class TestWbStocksInit:
    def test_source_dict_has_stocks(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'stocks' in instance.source_dict
        assert instance.source_dict['stocks']['func_name'] == instance.get_stocks

    def test_source_dict_stocks_history_same_func(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'stocks_history' in instance.source_dict
        assert instance.source_dict['stocks_history']['func_name'] == instance.get_stocks


@needs_credentials
@needs_httpx
class TestGetStocksLive:
    def test_returns_list(self, wb):
        result = wb.get_stocks()
        assert not isinstance(result, str), f'get_stocks вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_stocks()
        assert wb.err429 is False


# =====================================================================
#  wb_cards
# =====================================================================

@needs_httpx
class TestWbCardsInit:
    def test_source_dict_has_cards(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'cards' in instance.source_dict
        assert instance.source_dict['cards']['func_name'] == instance.get_cards


@needs_credentials
@needs_httpx
class TestGetCardsLive:
    def test_returns_list(self, wb):
        result = wb.get_cards()
        assert not isinstance(result, str), f'get_cards вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_not_empty(self, wb):
        result = wb.get_cards()
        assert isinstance(result, list) and len(result) > 0

    def test_no_err429(self, wb):
        wb.get_cards()
        assert wb.err429 is False


# =====================================================================
#  wb_adv_upd
# =====================================================================

@needs_httpx
class TestWbAdvUpdInit:
    def test_source_dict_has_adv_upd(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'adv_upd' in instance.source_dict
        assert instance.source_dict['adv_upd']['func_name'] == instance.get_adv_upd


@needs_credentials
@needs_httpx
class TestGetAdvUpdLive:
    def test_returns_list(self, wb):
        result = wb.get_adv_upd(YESTERDAY)
        assert not isinstance(result, str), f'get_adv_upd вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_adv_upd(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_paid_storage
# =====================================================================

@needs_httpx
class TestWbPaidStorageInit:
    def test_source_dict_has_paid_storage(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'paid_storage' in instance.source_dict
        assert instance.source_dict['paid_storage']['func_name'] == instance.get_paid_storage

    def test_helpers_exist(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert callable(getattr(instance, 'create_ps_report', None))
        assert callable(getattr(instance, 'ps_report_status', None))
        assert callable(getattr(instance, 'get_ps_report', None))


@needs_credentials
@needs_httpx
class TestGetPaidStorageLive:
    def test_returns_list(self, wb):
        result = wb.get_paid_storage(YESTERDAY)
        assert not isinstance(result, str), f'get_paid_storage вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_paid_storage(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_voronka_week  (требует таблицу wb_cards в ClickHouse — skip без CH)
# =====================================================================

@needs_httpx
class TestWbVoronkaWeekInit:
    def test_source_dict_has_voronka_week(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'voronka_week' in instance.source_dict
        assert instance.source_dict['voronka_week']['func_name'] == instance.get_voronka_week


# =====================================================================
#  wb_voronka_all
# =====================================================================

@needs_httpx
class TestWbVoronkaAllInit:
    def test_source_dict_has_voronka_all(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'voronka_all' in instance.source_dict
        assert instance.source_dict['voronka_all']['func_name'] == instance.get_voronka_all


@needs_credentials
@needs_httpx
class TestGetVoronkaAllLive:
    def test_returns_list(self, wb):
        result = wb.get_voronka_all(YESTERDAY)
        assert not isinstance(result, str), f'get_voronka_all вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_voronka_all(YESTERDAY)
        assert wb.err429 is False


# =====================================================================
#  wb_feedbacks
# =====================================================================

@needs_httpx
class TestWbFeedbacksInit:
    def test_source_dict_has_feedbacks(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert 'feedbacks' in instance.source_dict
        assert instance.source_dict['feedbacks']['func_name'] == instance.get_feedbacks

    def test_helper_get_chosen_feedbacks_exists(self):
        instance = WBbyDate(token='fake_token', add_name='test', start='2025-01-01')
        assert callable(getattr(instance, 'get_chosen_feedbacks', None))


@needs_credentials
@needs_httpx
class TestGetFeedbacksLive:
    def test_returns_list(self, wb):
        result = wb.get_feedbacks(YESTERDAY)
        assert not isinstance(result, str), f'get_feedbacks вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, wb):
        wb.get_feedbacks(YESTERDAY)
        assert wb.err429 is False


# #####################################################################
#  РУЧНОЙ ЗАПУСК:  python tests/test_wb.py
# #####################################################################

def _run_wb_realized(wb, run_test):
    print('\n--- wb_realized ---')
    result = wb.get_realized(WEEK_AGO)
    run_test('get_realized -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_orders(wb, run_test):
    print('\n--- wb_orders ---')
    result = wb.get_orders(YESTERDAY)
    run_test('get_orders -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_sbor_orders(wb, run_test):
    print('\n--- wb_sbor_orders ---')
    result = wb.get_sbor(YESTERDAY)
    run_test('get_sbor -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_sbor_status(wb, run_test):
    print('\n--- wb_sbor_status ---')
    result = wb.get_sbor_status(YESTERDAY)
    run_test('get_sbor_status -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_incomes(wb, run_test):
    print('\n--- wb_incomes ---')
    result = wb.get_incomes()
    run_test('get_incomes -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_excise(wb, run_test):
    print('\n--- wb_excise ---')
    result = wb.get_excise(YESTERDAY)
    run_test('get_excise -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_sales(wb, run_test):
    print('\n--- wb_sales ---')
    result = wb.get_sales(YESTERDAY)
    run_test('get_sales -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_orders_changes(wb, run_test):
    print('\n--- wb_orders_changes ---')
    result = wb.get_orders_changes(YESTERDAY)
    run_test('get_orders_changes -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_sales_changes(wb, run_test):
    print('\n--- wb_sales_changes ---')
    result = wb.get_sales_changes(YESTERDAY)
    run_test('get_sales_changes -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_stocks(wb, run_test):
    print('\n--- wb_stocks ---')
    result = wb.get_stocks()
    run_test('get_stocks -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_cards(wb, run_test):
    print('\n--- wb_cards ---')
    result = wb.get_cards()
    run_test('get_cards -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        run_test('not empty', lambda: (
            None if len(result) > 0
            else (_ for _ in ()).throw(AssertionError('список пустой'))
        ))
        print(f'  Получено записей: {len(result)}')


def _run_wb_adv_upd(wb, run_test):
    print('\n--- wb_adv_upd ---')
    result = wb.get_adv_upd(YESTERDAY)
    run_test('get_adv_upd -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_paid_storage(wb, run_test):
    print('\n--- wb_paid_storage ---')
    result = wb.get_paid_storage(YESTERDAY)
    run_test('get_paid_storage -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    for h in ['create_ps_report', 'ps_report_status', 'get_ps_report']:
        run_test(f'{h} callable', lambda n=h: (
            None if callable(getattr(wb, n, None))
            else (_ for _ in ()).throw(AssertionError(f'{n} не найден'))
        ))


def _run_wb_voronka_all(wb, run_test):
    print('\n--- wb_voronka_all ---')
    result = wb.get_voronka_all(YESTERDAY)
    run_test('get_voronka_all -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_wb_feedbacks(wb, run_test):
    print('\n--- wb_feedbacks ---')
    result = wb.get_feedbacks(YESTERDAY)
    run_test('get_feedbacks -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    run_test('get_chosen_feedbacks callable', lambda: (
        None if callable(getattr(wb, 'get_chosen_feedbacks', None))
        else (_ for _ in ()).throw(AssertionError('get_chosen_feedbacks не найден'))
    ))


RUNNERS = {
    'wb_realized':       _run_wb_realized,
    'wb_orders':         _run_wb_orders,
    'wb_sbor_orders':    _run_wb_sbor_orders,
    'wb_sbor_status':    _run_wb_sbor_status,
    'wb_incomes':        _run_wb_incomes,
    'wb_excise':         _run_wb_excise,
    'wb_sales':          _run_wb_sales,
    'wb_orders_changes': _run_wb_orders_changes,
    'wb_sales_changes':  _run_wb_sales_changes,
    'wb_stocks':         _run_wb_stocks,
    'wb_cards':          _run_wb_cards,
    'wb_adv_upd':        _run_wb_adv_upd,
    'wb_paid_storage':   _run_wb_paid_storage,
    'wb_voronka_all':    _run_wb_voronka_all,
    'wb_feedbacks':      _run_wb_feedbacks,
}


if __name__ == '__main__':

    # ===== ПАУЗА МЕЖДУ МЕТОДАМИ (секунды) =====
    DELAY_SECONDS = 90
    # =============================================

    # ===== ВЫБЕРИТЕ МЕТОДЫ ДЛЯ ТЕСТИРОВАНИЯ =====
    # Закомментируйте строки которые не хотите тестировать
    # voronka_week не включён — требует таблицу wb_cards в ClickHouse
    METHODS_TO_TEST = [
        'wb_realized',
        'wb_orders',
        'wb_sbor_orders',
        'wb_sbor_status',
        'wb_incomes',
        'wb_excise',
        'wb_sales',
        'wb_orders_changes',
        'wb_sales_changes',
        'wb_stocks',
        'wb_cards',
        'wb_adv_upd',
        'wb_paid_storage',
        'wb_voronka_all',
        'wb_feedbacks',
    ]
    # =============================================

    print('=' * 60)
    print('Тестирование WBbyDate — httpx-методы')
    print('=' * 60)

    if not credentials_filled:
        print(f'\nПРОПУСК: {skip_reason}')
        sys.exit(0)

    print(f'\nToken:             {WB_TOKEN[:6]}***')
    print(f'Дата (вчера):      {YESTERDAY}')
    print(f'Дата (неделя):     {WEEK_AGO}')
    print(f'Дата (start):      {START_DATE}')
    print(f'Методов к тесту:   {len(METHODS_TO_TEST)}')

    wb = WBbyDate(token=WB_TOKEN, add_name='test', start=START_DATE)
    passed = 0
    failed = 0

    def run_test(name, func):
        global passed, failed
        try:
            func()
            print(f'  OK   {name}')
            passed += 1
        except Exception as e:
            print(f'  FAIL {name}: {e}')
            failed += 1

    for i, method_name in enumerate(METHODS_TO_TEST):
        runner = RUNNERS.get(method_name)
        if runner:
            if i > 0 and DELAY_SECONDS > 0:
                print(f'\n  ... пауза {DELAY_SECONDS} сек ...')
                time.sleep(DELAY_SECONDS)
            runner(wb, run_test)
        else:
            print(f'\n  ??? Неизвестный метод: {method_name}')

    run_test('err429 still False', lambda: (
        None if wb.err429 is False
        else (_ for _ in ()).throw(AssertionError('err429 стал True'))
    ))

    wb.api.close()

    print()
    print('=' * 60)
    print(f'Результат: {passed} passed, {failed} failed')
    print('=' * 60)
    sys.exit(1 if failed else 0)
