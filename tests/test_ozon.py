import sys
import os
import time
import pytest
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin.ozon_by_date import OZONbyDate
from morin.base_client import BaseMarketplaceClient


# ===== ВСТАВЬТЕ СВОИ ДАННЫЕ =====
OZON_CLIENT_ID = ''
OZON_API_KEY = ''
# =================================

credentials_filled = bool(OZON_CLIENT_ID and OZON_API_KEY)
skip_reason = 'OZON_CLIENT_ID и OZON_API_KEY не заполнены — вставьте свои данные в начало файла'
needs_credentials = pytest.mark.skipif(not credentials_filled, reason=skip_reason)

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

today = datetime.now()
if today.day >= 6:
    REALIZATION_DATE = today.replace(day=6).strftime('%Y-%m-%d')
else:
    prev_month = today - relativedelta(months=1)
    REALIZATION_DATE = prev_month.replace(day=6).strftime('%Y-%m-%d')

if today.day >= 16:
    FINANCE_DATE = today.replace(day=16).strftime('%Y-%m-%d')
else:
    FINANCE_DATE = today.replace(day=1).strftime('%Y-%m-%d')


@pytest.fixture
def ozon():
    instance = OZONbyDate(clientid=OZON_CLIENT_ID, token=OZON_API_KEY, add_name='test')
    yield instance
    instance.api.close()


# =====================================================================
#  ozon_get_all_products
# =====================================================================

class TestOzonGetAllProductsInit:
    def test_api_is_base_marketplace_client(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert isinstance(instance.api, BaseMarketplaceClient)

    def test_base_url_contains_ozon(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        base_url = str(instance.api.client.base_url)
        assert 'api-seller.ozon.ru' in base_url

    def test_headers_contain_client_id(self):
        instance = OZONbyDate(clientid='my_client', token='my_token', add_name='test')
        headers = dict(instance.api.client.headers)
        assert headers.get('client-id') == 'my_client'

    def test_headers_contain_api_key(self):
        instance = OZONbyDate(clientid='my_client', token='my_token', add_name='test')
        headers = dict(instance.api.client.headers)
        assert headers.get('api-key') == 'my_token'

    def test_headers_contain_content_type(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        headers = dict(instance.api.client.headers)
        assert headers.get('content-type') == 'application/json'

    def test_err429_default_false(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert instance.err429 is False

    def test_api_has_request_method(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert hasattr(instance.api, '_request') and callable(instance.api._request)

    def test_api_has_request_raw_method(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert hasattr(instance.api, '_request_raw') and callable(instance.api._request_raw)

    def test_api_has_close_method(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert hasattr(instance.api, 'close') and callable(instance.api.close)

    def test_source_dict_has_products(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'products' in instance.source_dict
        assert instance.source_dict['products']['func_name'] == instance.get_all_products


@needs_credentials
class TestGetAllProductsLive:
    def test_returns_list(self, ozon):
        result = ozon.get_all_products()
        assert not isinstance(result, str), f'get_all_products вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_not_empty(self, ozon):
        result = ozon.get_all_products()
        assert isinstance(result, list) and len(result) > 0

    def test_item_has_product_id(self, ozon):
        result = ozon.get_all_products()
        assert isinstance(result, list) and len(result) > 0
        assert 'product_id' in result[0]

    def test_item_has_offer_id(self, ozon):
        result = ozon.get_all_products()
        assert isinstance(result, list) and len(result) > 0
        assert 'offer_id' in result[0]

    def test_product_id_is_int(self, ozon):
        result = ozon.get_all_products()
        assert isinstance(result, list) and len(result) > 0
        assert isinstance(result[0]['product_id'], int)

    def test_no_err429(self, ozon):
        ozon.get_all_products()
        assert ozon.err429 is False


# =====================================================================
#  ozon_transactions
# =====================================================================

class TestOzonTransactionsInit:
    def test_source_dict_has_transactions(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'transactions' in instance.source_dict
        assert instance.source_dict['transactions']['func_name'] == instance.get_transactions


@needs_credentials
class TestGetTransactionsLive:
    def test_returns_list(self, ozon):
        result = ozon.get_transactions(YESTERDAY)
        assert not isinstance(result, str), f'get_transactions вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_transactions(YESTERDAY)
        assert ozon.err429 is False


@needs_credentials
class TestGetTransactionPageCountLive:
    def test_returns_int(self, ozon):
        result = ozon.get_transaction_page_count(YESTERDAY)
        assert not isinstance(result, str), f'get_transaction_page_count вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, int)

    def test_non_negative(self, ozon):
        result = ozon.get_transaction_page_count(YESTERDAY)
        assert isinstance(result, int) and result >= 0


# =====================================================================
#  ozon_stocks  (stocks + stocks_history — одна функция)
# =====================================================================

class TestOzonStocksInit:
    def test_source_dict_has_stocks(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'stocks' in instance.source_dict
        assert instance.source_dict['stocks']['func_name'] == instance.get_stock_on_warehouses

    def test_source_dict_stocks_history_same_func(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'stocks_history' in instance.source_dict
        assert instance.source_dict['stocks_history']['func_name'] == instance.get_stock_on_warehouses


@needs_credentials
class TestGetStockOnWarehousesLive:
    def test_returns_list(self, ozon):
        result = ozon.get_stock_on_warehouses()
        assert not isinstance(result, str), f'get_stock_on_warehouses вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_stock_on_warehouses()
        assert ozon.err429 is False


# =====================================================================
#  ozon_stocks_sku  (stocks_sku + stocks_sku_history — одна функция)
# =====================================================================

class TestOzonStocksSkuInit:
    def test_source_dict_has_stocks_sku(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'stocks_sku' in instance.source_dict
        assert instance.source_dict['stocks_sku']['func_name'] == instance.get_stocks_sku

    def test_source_dict_stocks_sku_history_same_func(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'stocks_sku_history' in instance.source_dict
        assert instance.source_dict['stocks_sku_history']['func_name'] == instance.get_stocks_sku


@needs_credentials
class TestGetStocksSkuLive:
    def test_returns_list(self, ozon):
        result = ozon.get_stocks_sku()
        assert not isinstance(result, str), f'get_stocks_sku вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_stocks_sku()
        assert ozon.err429 is False


@needs_credentials
class TestGetStocksSkuHelpersLive:
    def test_create_products_report_callable(self, ozon):
        assert callable(getattr(ozon, 'create_products_report', None))

    def test_get_report_info_callable(self, ozon):
        assert callable(getattr(ozon, 'get_report_info', None))

    def test_csv_to_dict_list_callable(self, ozon):
        assert callable(getattr(ozon, 'csv_to_dict_list', None))

    def test_get_all_skus_callable(self, ozon):
        assert callable(getattr(ozon, 'get_all_skus', None))

    def test_get_ozon_stocks_callable(self, ozon):
        assert callable(getattr(ozon, 'get_ozon_stocks', None))


# =====================================================================
#  ozon_products_info
# =====================================================================

class TestOzonProductsInfoInit:
    def test_source_dict_has_products_info(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'products_info' in instance.source_dict
        assert instance.source_dict['products_info']['func_name'] == instance.get_all_products_info


@needs_credentials
class TestGetAllProductsInfoLive:
    def test_returns_list(self, ozon):
        result = ozon.get_all_products_info()
        assert not isinstance(result, str), f'get_all_products_info вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_not_empty(self, ozon):
        result = ozon.get_all_products_info()
        assert isinstance(result, list) and len(result) > 0

    def test_no_err429(self, ozon):
        ozon.get_all_products_info()
        assert ozon.err429 is False


# =====================================================================
#  ozon_returns
# =====================================================================

class TestOzonReturnsInit:
    def test_source_dict_has_returns(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'returns' in instance.source_dict
        assert instance.source_dict['returns']['func_name'] == instance.get_all_returns


@needs_credentials
class TestGetAllReturnsLive:
    def test_returns_list(self, ozon):
        result = ozon.get_all_returns()
        assert not isinstance(result, str), f'get_all_returns вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_all_returns()
        assert ozon.err429 is False


# =====================================================================
#  ozon_returns_days
# =====================================================================

class TestOzonReturnsDaysInit:
    def test_source_dict_has_returns_days(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'returns_days' in instance.source_dict
        assert instance.source_dict['returns_days']['func_name'] == instance.get_returns


@needs_credentials
class TestGetReturnsLive:
    def test_returns_list(self, ozon):
        result = ozon.get_returns(YESTERDAY)
        assert not isinstance(result, str), f'get_returns вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_returns(YESTERDAY)
        assert ozon.err429 is False


# =====================================================================
#  ozon_realization
# =====================================================================

class TestOzonRealizationInit:
    def test_source_dict_has_realization(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'realization' in instance.source_dict
        assert instance.source_dict['realization']['func_name'] == instance.get_realization


@needs_credentials
class TestGetRealizationLive:
    def test_returns_list(self, ozon):
        result = ozon.get_realization(REALIZATION_DATE)
        assert not isinstance(result, str), f'get_realization вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_realization(REALIZATION_DATE)
        assert ozon.err429 is False


# =====================================================================
#  ozon_realization_posting
# =====================================================================

class TestOzonRealizationPostingInit:
    def test_source_dict_has_realization_posting(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'realization_posting' in instance.source_dict
        assert instance.source_dict['realization_posting']['func_name'] == instance.get_realization_posting


@needs_credentials
class TestGetRealizationPostingLive:
    def test_returns_list(self, ozon):
        result = ozon.get_realization_posting(REALIZATION_DATE)
        assert not isinstance(result, str), f'get_realization_posting вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_realization_posting(REALIZATION_DATE)
        assert ozon.err429 is False


# =====================================================================
#  ozon_postings_fbo
# =====================================================================

class TestOzonPostingsFboInit:
    def test_source_dict_has_postings_fbo(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'postings_fbo' in instance.source_dict
        assert instance.source_dict['postings_fbo']['func_name'] == instance.get_postings_fbo


@needs_credentials
class TestGetPostingsFboLive:
    def test_returns_list(self, ozon):
        result = ozon.get_postings_fbo(YESTERDAY)
        assert not isinstance(result, str), f'get_postings_fbo вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_postings_fbo(YESTERDAY)
        assert ozon.err429 is False


# =====================================================================
#  ozon_postings_fbs_rep
# =====================================================================

class TestOzonPostingsFbsRepInit:
    def test_source_dict_has_postings_fbs_rep(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'postings_fbs_rep' in instance.source_dict
        assert instance.source_dict['postings_fbs_rep']['func_name'] == instance.get_postings_fbs_report


@needs_credentials
class TestGetPostingsFbsReportLive:
    def test_returns_list(self, ozon):
        result = ozon.get_postings_fbs_report(YESTERDAY)
        assert not isinstance(result, str), f'get_postings_fbs_report вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_postings_fbs_report(YESTERDAY)
        assert ozon.err429 is False


@needs_credentials
class TestPostingsFbsReportHelpersLive:
    def test_create_postings_report_callable(self, ozon):
        assert callable(getattr(ozon, 'create_postings_report', None))

    def test_get_report_info_callable(self, ozon):
        assert callable(getattr(ozon, 'get_report_info', None))

    def test_csv_to_dict_list_callable(self, ozon):
        assert callable(getattr(ozon, 'csv_to_dict_list', None))


# =====================================================================
#  ozon_finance_details
# =====================================================================

class TestOzonFinanceDetailsInit:
    def test_source_dict_has_finance_details(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'finance_details' in instance.source_dict
        assert instance.source_dict['finance_details']['func_name'] == instance.get_finance_details


@needs_credentials
class TestGetFinanceDetailsLive:
    def test_returns_list(self, ozon):
        result = ozon.get_finance_details(FINANCE_DATE)
        assert not isinstance(result, str), f'get_finance_details вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_finance_details(FINANCE_DATE)
        assert ozon.err429 is False


@needs_credentials
class TestFinanceDetailsHelpersLive:
    def test_get_date_range_returns_tuple(self, ozon):
        result = ozon.get_date_range(FINANCE_DATE)
        assert not isinstance(result, str), f'get_date_range вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, tuple) and len(result) == 2

    def test_get_finance_total_pages_returns_int(self, ozon):
        start_date, end_date = ozon.get_date_range(FINANCE_DATE)
        result = ozon.get_finance_total_pages(start_date, end_date)
        assert not isinstance(result, str), f'get_finance_total_pages вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, int)


# =====================================================================
#  ozon_finance_cashflow
# =====================================================================

class TestOzonFinanceCashflowInit:
    def test_source_dict_has_finance_cashflow(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'finance_cashflow' in instance.source_dict
        assert instance.source_dict['finance_cashflow']['func_name'] == instance.get_finance_cashflow


@needs_credentials
class TestGetFinanceCashflowLive:
    def test_returns_list(self, ozon):
        result = ozon.get_finance_cashflow(FINANCE_DATE)
        assert not isinstance(result, str), f'get_finance_cashflow вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_finance_cashflow(FINANCE_DATE)
        assert ozon.err429 is False


@needs_credentials
class TestFinanceCashflowHelpersLive:
    def test_get_date_range_returns_tuple(self, ozon):
        result = ozon.get_date_range(FINANCE_DATE)
        assert isinstance(result, tuple) and len(result) == 2

    def test_get_finance_total_pages_returns_int(self, ozon):
        start_date, end_date = ozon.get_date_range(FINANCE_DATE)
        result = ozon.get_finance_total_pages(start_date, end_date)
        assert isinstance(result, int)


# =====================================================================
#  ozon_products_buyout
# =====================================================================

class TestOzonProductsBuyoutInit:
    def test_source_dict_has_products_buyout(self):
        instance = OZONbyDate(clientid='fake_id', token='fake_key', add_name='test')
        assert 'products_buyout' in instance.source_dict
        assert instance.source_dict['products_buyout']['func_name'] == instance.get_products_buyout


@needs_credentials
class TestGetProductsBuyoutLive:
    def test_returns_list(self, ozon):
        result = ozon.get_products_buyout(YESTERDAY)
        assert not isinstance(result, str), f'get_products_buyout вернул строку (ошибка): {result[:300]}'
        assert isinstance(result, list)

    def test_no_err429(self, ozon):
        ozon.get_products_buyout(YESTERDAY)
        assert ozon.err429 is False


# #####################################################################
#  РУЧНОЙ ЗАПУСК:  python tests/test_ozon.py
# #####################################################################

def _run_ozon_get_all_products(ozon, run_test):
    print('\n--- ozon_get_all_products ---')
    result = ozon.get_all_products()
    run_test('get_all_products -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        run_test('not empty', lambda: (
            None if len(result) > 0
            else (_ for _ in ()).throw(AssertionError('список пустой'))
        ))
        if len(result) > 0:
            run_test('item has product_id', lambda: (
                None if 'product_id' in result[0]
                else (_ for _ in ()).throw(AssertionError(f'keys={list(result[0].keys())}'))
            ))
            run_test('item has offer_id', lambda: (
                None if 'offer_id' in result[0]
                else (_ for _ in ()).throw(AssertionError(f'keys={list(result[0].keys())}'))
            ))
        print(f'  Всего товаров: {len(result)}')


def _run_ozon_transactions(ozon, run_test):
    print('\n--- ozon_transactions ---')
    result = ozon.get_transactions(YESTERDAY)
    run_test('get_transactions -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    pages = ozon.get_transaction_page_count(YESTERDAY)
    run_test('get_transaction_page_count -> int >= 0', lambda: (
        None if isinstance(pages, int) and pages >= 0
        else (_ for _ in ()).throw(AssertionError(f'type={type(pages).__name__}, value={pages}'))
    ))
    print(f'  Страниц: {pages}')


def _run_ozon_stocks(ozon, run_test):
    print('\n--- ozon_stocks ---')
    result = ozon.get_stock_on_warehouses()
    run_test('get_stock_on_warehouses -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_ozon_stocks_sku(ozon, run_test):
    print('\n--- ozon_stocks_sku ---')
    result = ozon.get_stocks_sku()
    run_test('get_stocks_sku -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    for h in ['create_products_report', 'get_report_info', 'csv_to_dict_list', 'get_all_skus', 'get_ozon_stocks']:
        run_test(f'{h} callable', lambda n=h: (
            None if callable(getattr(ozon, n, None))
            else (_ for _ in ()).throw(AssertionError(f'{n} не найден'))
        ))


def _run_ozon_products_info(ozon, run_test):
    print('\n--- ozon_products_info ---')
    result = ozon.get_all_products_info()
    run_test('get_all_products_info -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        run_test('not empty', lambda: (
            None if len(result) > 0
            else (_ for _ in ()).throw(AssertionError('список пустой'))
        ))
        print(f'  Получено записей: {len(result)}')


def _run_ozon_returns(ozon, run_test):
    print('\n--- ozon_returns ---')
    result = ozon.get_all_returns()
    run_test('get_all_returns -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_ozon_returns_days(ozon, run_test):
    print('\n--- ozon_returns_days ---')
    result = ozon.get_returns(YESTERDAY)
    run_test('get_returns -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_ozon_realization(ozon, run_test):
    print('\n--- ozon_realization ---')
    result = ozon.get_realization(REALIZATION_DATE)
    run_test('get_realization -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_ozon_realization_posting(ozon, run_test):
    print('\n--- ozon_realization_posting ---')
    result = ozon.get_realization_posting(REALIZATION_DATE)
    run_test('get_realization_posting -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_ozon_postings_fbo(ozon, run_test):
    print('\n--- ozon_postings_fbo ---')
    result = ozon.get_postings_fbo(YESTERDAY)
    run_test('get_postings_fbo -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


def _run_ozon_postings_fbs_rep(ozon, run_test):
    print('\n--- ozon_postings_fbs_rep ---')
    result = ozon.get_postings_fbs_report(YESTERDAY)
    run_test('get_postings_fbs_report -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    for h in ['create_postings_report', 'get_report_info', 'csv_to_dict_list']:
        run_test(f'{h} callable', lambda n=h: (
            None if callable(getattr(ozon, n, None))
            else (_ for _ in ()).throw(AssertionError(f'{n} не найден'))
        ))


def _run_ozon_finance_details(ozon, run_test):
    print('\n--- ozon_finance_details ---')
    result = ozon.get_finance_details(FINANCE_DATE)
    run_test('get_finance_details -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    date_range = ozon.get_date_range(FINANCE_DATE)
    run_test('get_date_range -> tuple of 2', lambda: (
        None if isinstance(date_range, tuple) and len(date_range) == 2
        else (_ for _ in ()).throw(AssertionError(f'type={type(date_range).__name__}, value={str(date_range)[:200]}'))
    ))
    if isinstance(date_range, tuple) and len(date_range) == 2:
        pages = ozon.get_finance_total_pages(date_range[0], date_range[1])
        run_test('get_finance_total_pages -> int', lambda: (
            None if isinstance(pages, int)
            else (_ for _ in ()).throw(AssertionError(f'type={type(pages).__name__}, value={str(pages)[:200]}'))
        ))
        print(f'  Страниц: {pages}')


def _run_ozon_finance_cashflow(ozon, run_test):
    print('\n--- ozon_finance_cashflow ---')
    result = ozon.get_finance_cashflow(FINANCE_DATE)
    run_test('get_finance_cashflow -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')
    date_range = ozon.get_date_range(FINANCE_DATE)
    run_test('get_date_range -> tuple of 2', lambda: (
        None if isinstance(date_range, tuple) and len(date_range) == 2
        else (_ for _ in ()).throw(AssertionError(f'type={type(date_range).__name__}'))
    ))
    if isinstance(date_range, tuple) and len(date_range) == 2:
        pages = ozon.get_finance_total_pages(date_range[0], date_range[1])
        run_test('get_finance_total_pages -> int', lambda: (
            None if isinstance(pages, int)
            else (_ for _ in ()).throw(AssertionError(f'type={type(pages).__name__}'))
        ))
        print(f'  Страниц: {pages}')


def _run_ozon_products_buyout(ozon, run_test):
    print('\n--- ozon_products_buyout ---')
    result = ozon.get_products_buyout(YESTERDAY)
    run_test('get_products_buyout -> list', lambda: (
        None if isinstance(result, list)
        else (_ for _ in ()).throw(AssertionError(f'type={type(result).__name__}, value={str(result)[:200]}'))
    ))
    if isinstance(result, list):
        print(f'  Получено записей: {len(result)}')


RUNNERS = {
    'ozon_get_all_products':    _run_ozon_get_all_products,
    'ozon_transactions':        _run_ozon_transactions,
    'ozon_stocks':              _run_ozon_stocks,
    'ozon_stocks_sku':          _run_ozon_stocks_sku,
    'ozon_products_info':       _run_ozon_products_info,
    'ozon_returns':             _run_ozon_returns,
    'ozon_returns_days':        _run_ozon_returns_days,
    'ozon_realization':         _run_ozon_realization,
    'ozon_realization_posting': _run_ozon_realization_posting,
    'ozon_postings_fbo':        _run_ozon_postings_fbo,
    'ozon_postings_fbs_rep':    _run_ozon_postings_fbs_rep,
    'ozon_finance_details':     _run_ozon_finance_details,
    'ozon_finance_cashflow':    _run_ozon_finance_cashflow,
    'ozon_products_buyout':     _run_ozon_products_buyout,
}


if __name__ == '__main__':

    # ===== ПАУЗА МЕЖДУ МЕТОДАМИ (секунды) =====
    DELAY_SECONDS = 60
    # =============================================

    # ===== ВЫБЕРИТЕ МЕТОДЫ ДЛЯ ТЕСТИРОВАНИЯ =====
    # Закомментируйте строки которые не хотите тестировать
    METHODS_TO_TEST = [
        'ozon_get_all_products',
        'ozon_transactions',
        'ozon_stocks',
        'ozon_stocks_sku',
        'ozon_products_info',
        'ozon_returns',
        'ozon_returns_days',
        'ozon_realization',
        'ozon_realization_posting',
        'ozon_postings_fbo',
        'ozon_postings_fbs_rep',
        'ozon_finance_details',
        'ozon_finance_cashflow',
        'ozon_products_buyout',
    ]
    # =============================================

    print('=' * 60)
    print('Тестирование OZONbyDate — httpx-методы')
    print('=' * 60)

    if not credentials_filled:
        print(f'\nПРОПУСК: {skip_reason}')
        sys.exit(0)

    print(f'\nClient-Id:         {OZON_CLIENT_ID[:6]}***')
    print(f'Api-Key:           {OZON_API_KEY[:6]}***')
    print(f'Дата (вчера):      {YESTERDAY}')
    print(f'Дата (реализация): {REALIZATION_DATE}')
    print(f'Дата (финансы):    {FINANCE_DATE}')
    print(f'Методов к тесту:   {len(METHODS_TO_TEST)}')

    ozon = OZONbyDate(clientid=OZON_CLIENT_ID, token=OZON_API_KEY, add_name='test')
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
            runner(ozon, run_test)
        else:
            print(f'\n  ??? Неизвестный метод: {method_name}')

    run_test('err429 still False', lambda: (
        None if ozon.err429 is False
        else (_ for _ in ()).throw(AssertionError('err429 стал True'))
    ))

    ozon.api.close()

    print()
    print('=' * 60)
    print(f'Результат: {passed} passed, {failed} failed')
    print('=' * 60)
    sys.exit(1 if failed else 0)
