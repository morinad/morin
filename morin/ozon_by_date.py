from .common import Common
from .clickhouse import Clickhouse
from .ozon_reklama import OZONreklama
from .base_client import BaseMarketplaceClient
import requests
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
import os
import csv
from dateutil import parser
import time
import hashlib
from io import StringIO
import json
from dateutil.relativedelta import relativedelta


class OZONbyDate:
    def __init__(self,  bot_token:str = '', chats:str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                                  add_name: str = '', clientid:str = '', token: str  = '',  start: str = '', backfill_days: int = 0, reports :str = ''):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.clientid = clientid
        self.token = token
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.subd = subd
        self.add_name = self.common.transliterate_key(add_name)
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.platform = 'ozon'
        self.err429 = False
        self.api = BaseMarketplaceClient(
            base_url='https://api-seller.ozon.ru',
            headers={
                'Client-Id': self.clientid,
                'Api-Key': self.token,
                'Content-Type': 'application/json'
            },
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )
        self.common.log_func(self.bot_token, self.chat_list, f'Платформа: OZON. Имя: {self.add_name}. HTTP-клиент: httpx', 1)
        self.source_dict = {
            'transactions': {
                'platform': 'ozon',
                'report_name': 'transactions',
                'upload_table': 'transactions',
                'func_name': self.get_transactions,
                'uniq_columns': 'operation_date,operation_id',
                'partitions': 'operation_date',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily', # '2dayOfMonth,Friday'
                'delay': 30
            },
            'stocks': {
                'platform': 'ozon',
                'report_name': 'stocks',
                'upload_table': 'stocks',
                'func_name': self.get_stock_on_warehouses,
                'uniq_columns': 'sku',
                'partitions': 'warehouse_name',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'stocks_sku': {
                'platform': 'ozon',
                'report_name': 'stocks_sku',
                'upload_table': 'stocks_sku',
                'func_name': self.get_stocks_sku,
                'uniq_columns': 'sku',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'stocks_sku_history': {
                'platform': 'ozon',
                'report_name': 'stocks_sku_history',
                'upload_table': 'stocks_sku_history',
                'func_name': self.get_stocks_sku,
                'uniq_columns': 'sku',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'stocks_history': {
                'platform': 'ozon',
                'report_name': 'stocks_history',
                'upload_table': 'stocks_history',
                'func_name': self.get_stock_on_warehouses,
                'uniq_columns': 'sku',
                'partitions': 'warehouse_name',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'products': {
                'platform': 'ozon',
                'report_name': 'products',
                'upload_table': 'products',
                'func_name': self.get_all_products,
                'uniq_columns': 'product_id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'products_info': {
                'platform': 'ozon',
                'report_name': 'products_info',
                'upload_table': 'products_info',
                'func_name': self.get_all_products_info,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'returns': {
                'platform': 'ozon',
                'report_name': 'returns',
                'upload_table': 'returns',
                'func_name': self.get_all_returns,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'returns_days': {
                'platform': 'ozon',
                'report_name': 'returns_days',
                'upload_table': 'returns_days',
                'func_name': self.get_returns,
                'uniq_columns': 'id,company_id,order_id,logistic_return_date',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'realization': {
                'platform': 'ozon',
                'report_name': 'realization',
                'upload_table': 'realization',
                'func_name': self.get_realization,
                'uniq_columns': 'year_month,rowNumber',
                'partitions': 'year_month',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': '6',  # '2,Friday'
                'delay': 30
            },
            'realization_posting': {
                'platform': 'ozon',
                'report_name': 'realization_posting',
                'upload_table': 'realization_posting',
                'func_name': self.get_realization_posting,
                'uniq_columns': 'year_month,row_number',
                'partitions': 'year_month',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': '6',  # '2,Friday'
                'delay': 30
            },
            'postings_fbo': {
                'platform': 'ozon',
                'report_name': 'postings_fbo',
                'upload_table': 'postings_fbo',
                'func_name': self.get_postings_fbo,
                'uniq_columns': 'posting_number,created_at',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2,Friday'
                'delay': 30
            },
            'postings_fbs_rep': {
                'platform': 'ozon',
                'report_name': 'postings_fbs_rep',
                'upload_table': 'postings_fbs_rep',
                'func_name': self.get_postings_fbs_report,
                'uniq_columns': 'load_date',
                'partitions': 'load_date',
                'merge_type': 'MergeTree', # тут надо выбрать схему обновления
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2,Friday'
                'delay': 30
            },
            'finance_details': {
                'platform': 'ozon',
                'report_name': 'finance_details',
                'upload_table': 'finance_details',
                'func_name': self.get_finance_details,
                'uniq_columns': 'period_id',
                'partitions': 'period_id',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': '1,16',  # '2,Friday'
                'delay': 30
            },
            'finance_cashflow': {
                'platform': 'ozon',
                'report_name': 'finance_cashflow',
                'upload_table': 'finance_cashflow',
                'func_name': self.get_finance_cashflow,
                'uniq_columns': 'period_id',
                'partitions': 'period_id',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': '1,16',  # '2,Friday'
                'delay': 30
            },
            'products_buyout': {
            'platform': 'ozon',
            'report_name': 'products_buyout',
            'upload_table': 'products_buyout',
            'func_name': self.get_products_buyout,
            'uniq_columns': 'offer_id,sku,posting_number',
            'partitions': '',
            'merge_type': 'ReplacingMergeTree(timeStamp)',
            'refresh_type': 'nothing',
            'history': True,
            'frequency': 'daily',  # '2,Friday'
            'delay': 30
        },
        }

    def _log_ok(self, func_name, date=''):
        message = f'Платформа: {self.platform.upper()}. Имя: {self.add_name}. Дата: {date}. Функция: {func_name}. Результат: ОК'
        self.common.log_func(self.bot_token, self.chat_list, message, 1)

    def _log_err(self, func_name, date='', error=''):
        if hasattr(self, 'api') and hasattr(self.api, 'err429') and self.api.err429:
            self.err429 = True
        message = f'Платформа: {self.platform.upper()}. Имя: {self.add_name}. Дата: {date}. Функция: {func_name}. Ошибка: {error}.'
        self.common.log_func(self.bot_token, self.chat_list, message, 3)
        return message

    def create_postings_report(self, date, report_type):
        try:
            data = self.api._request('POST', '/v1/report/postings/create', json={
                'filter': {
                    'processed_at_from': f'{date}T00:00:00.000Z',
                    'processed_at_to': f'{date}T23:59:59.999Z',
                    'delivery_schema': [report_type]
                },
                'language': 'RU'
            })
            report_code = data['result']['code']
            self._log_ok('create_postings_report', date)
            return report_code
        except Exception as e:
            return self._log_err('create_postings_report', date, e)


    def create_products_report(self):
        try:
            data = self.api._request('POST', '/v1/report/products/create', json={
                'language': 'RU',
                'visibility': 'ALL'
            })
            report_code = data['result']['code']
            self._log_ok('create_products_report')
            return report_code
        except Exception as e:
            return self._log_err('create_products_report', '', e)


    def get_report_info(self, report_code):
        try:
            data = self.api._request('POST', '/v1/report/info', json={
                'code': report_code
            })
            return data
        except Exception as e:
            return self._log_err('get_report_info', '', e)

    def get_ozon_stocks(self, skus):
        try:
            batch_size = 100
            all_results = []
            for i in range(0, len(skus), batch_size):
                batch = skus[i:i + batch_size]
                batch_str = [str(x) for x in batch if x != 0 and x != '0']
                if batch_str:
                    data = self.api._request('POST', '/v1/analytics/stocks', json={
                        'skus': batch_str
                    })
                    items = data.get('items', [])
                    all_results.extend(items)
                    time.sleep(40)
            self._log_ok('get_ozon_stocks')
            return all_results
        except Exception as e:
            return self._log_err('get_ozon_stocks', '', e)

    def get_all_skus(self, data_list):
        try:
            sku_list = []
            for k in data_list:
                try:
                    sku_list.append(k['SKU'])
                except:
                    pass
            return sku_list
        except Exception as e:
            return self._log_err('get_all_skus', '', e)


    def csv_to_dict_list(self, url):
        try:
            try:
                import httpx as _httpx
                response = _httpx.get(url, timeout=60.0)
            except ImportError:
                import requests as _requests
                response = _requests.get(url, timeout=60)
            response.raise_for_status()
            clean_text = response.text.lstrip('\ufeff')
            csv_content = StringIO(clean_text)
            csv_reader = csv.reader(csv_content, delimiter=';')
            headers = next(csv_reader, None)
            result = [dict(zip(headers, row)) for row in csv_reader if row]
            if not headers or not result:
                message = f'Платформа: OZON. Имя: {self.add_name}. Функция: csv_to_dict_list. ПУСТОЙ ОТЧЁТ.'
                self.common.log_func(self.bot_token, self.chat_list, message, 2)
            return result
        except Exception as e:
            return self._log_err('csv_to_dict_list', '', e)

    def get_postings_fbs_report(self, date):
        try:
            new_report = self.create_postings_report(date,'fbs')
            for k in range(20):
                time.sleep(10)
                get_link = self.get_report_info(new_report)
                if get_link['result']['status'] == 'success':
                    data = self.common.transliterate_dict_keys_in_list(self.csv_to_dict_list(get_link['result']['file']))
                    for elem in data:
                        elem['load_date'] = date
                    return data
            self._log_ok('get_postings_fbs_report', date)
        except Exception as e:
            return self._log_err('get_postings_fbs_report', date, e)


    def get_stocks_sku(self, date=''):
        try:
            new_report = self.create_products_report()
            for k in range(50):
                time.sleep(40)
                get_link = self.get_report_info(new_report)
                if get_link['result']['status'] == 'success':
                    data = self.csv_to_dict_list(get_link['result']['file'])
                    skus = self.get_all_skus(data)
                    result = self.get_ozon_stocks(skus)
                    for elem in result:
                        elem['load_date'] = date
                    return result
            self._log_ok('get_stocks_sku', date)
        except Exception as e:
            return self._log_err('get_stocks_sku', date, e)

    def get_transaction_page_count(self, date):
        try:
            data = self.api._request('POST', '/v3/finance/transaction/list', json={
                'filter': {
                    'date': {'from': f'{date}T00:00:00.000Z', 'to': f'{date}T23:59:59.999Z'},
                    'operation_type': [],
                    'posting_number': '',
                    'transaction_type': 'all'
                },
                'page': 1,
                'page_size': 1000
            })
            return data['result']['page_count']
        except Exception as e:
            return self._log_err('get_transaction_page_count', date, e)

    def get_transactions(self, date):
        try:
            page_count = int(self.get_transaction_page_count(date))
            operations = []
            for page in range(1, page_count + 1):
                data = self.api._request('POST', '/v3/finance/transaction/list', json={
                    'filter': {
                        'date': {'from': f'{date}T00:00:00.000Z', 'to': f'{date}T23:59:59.999Z'},
                        'operation_type': [],
                        'posting_number': '',
                        'transaction_type': 'all'
                    },
                    'page': page,
                    'page_size': 1000
                })
                operations += data['result']['operations']
                time.sleep(2)
            self._log_ok('get_transactions', date)
            return operations
        except Exception as e:
            return self._log_err('get_transactions', date, e)

    def get_stock_on_warehouses(self, date=''):
        try:
            all_rows = []
            offset = 0
            limit = 1000
            while True:
                data = self.api._request('POST', '/v2/analytics/stock_on_warehouses', json={
                    'limit': limit,
                    'offset': offset,
                    'warehouse_type': 'ALL'
                })
                rows = data.get('result', {}).get('rows', [])
                if not rows:
                    break
                all_rows.extend(rows)
                offset += limit
            self._log_ok('get_stock_on_warehouses', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_stock_on_warehouses', date, e)

    def get_all_products(self, date=''):
        try:
            all_items = []
            last_id = ""
            limit = 1000
            while True:
                data = self.api._request('POST', '/v3/product/list', json={
                    'last_id': last_id,
                    'limit': limit,
                    'filter': {}
                })
                result = data.get('result', {})
                items = result.get('items', [])
                if not items:
                    break
                all_items.extend(items)
                if len(items) < limit:
                    break
                last_id = result.get('last_id', '')
            self._log_ok('get_all_products', date)
            return all_items
        except Exception as e:
            return self._log_err('get_all_products', date, e)

    def get_all_products_info(self, date=''):
        try:
            all_items = []
            last_id = ''
            limit = 1000
            while True:
                data = self.api._request('POST', '/v3/product/list', json={
                    'last_id': last_id,
                    'limit': limit,
                    'filter': {}
                })
                result = data.get('result', {})
                items = result.get('items', [])
                if not items:
                    break
                product_ids = [item['product_id'] for item in items]
                data2 = self.api._request('POST', '/v3/product/info/list', json={
                    'product_id': product_ids
                })
                items2 = data2.get('items', [])
                if not items2:
                    break
                all_items.extend(items2)
                if len(items) < limit:
                    break
                last_id = result.get('last_id', '')
            self._log_ok('get_all_products_info', date)
            return self.common.spread_table(self.common.spread_table(all_items))
        except Exception as e:
            return self._log_err('get_all_products_info', date, e)


    def get_all_returns(self, date=''):
        try:
            all_returns = []
            last_id = 0
            limit = 500
            while True:
                data = self.api._request('POST', '/v1/returns/list', json={
                    'last_id': last_id,
                    'limit': limit
                })
                returns = data.get('returns', [])
                if not returns:
                    break
                all_returns.extend(returns)
                if len(returns) < limit:
                    break
                last_id = int(returns[-1]['id'])
            self._log_ok('get_all_returns', date)
            return all_returns
        except Exception as e:
            return self._log_err('get_all_returns', date, e)

    def get_returns(self, date):
        try:
            all_returns = []
            last_id = 0
            limit = 500
            while True:
                data = self.api._request('POST', '/v1/returns/list', json={
                    'filter': {
                        'logistic_return_date': {
                            'time_from': f'{date}T00:00:00Z',
                            'time_to': f'{date}T23:59:59Z'
                        }
                    },
                    'last_id': last_id,
                    'limit': limit
                })
                returns = data.get('returns', [])
                if not returns:
                    break
                all_returns.extend(returns)
                if len(returns) < limit:
                    break
                last_id = int(returns[-1]['id'])
            all_returns = self.common.spread_table(self.common.spread_table(all_returns))
            for item in all_returns:
                item['logistic_return_date'] = date
            self._log_ok('get_returns', date)
            return all_returns
        except Exception as e:
            return self._log_err('get_returns', date, e)


    def get_realization(self, date):
        try:
            real_date = datetime.strptime(date, "%Y-%m-%d")
            last_month_date = real_date - relativedelta(months=1)
            previous_month = last_month_date.month
            previous_year = last_month_date.year
            yyyy_mm = f"{previous_year}-{str(previous_month).zfill(2)}-01"
            resp = self.api._request('POST', '/v2/finance/realization', json={
                'month': previous_month,
                'year': previous_year
            })
            result = resp.get('result', {}).get('rows', [])
            for row in result:
                row['year_month'] = yyyy_mm
            final_result = self.common.spread_table(result)
            self._log_ok('get_realization', date)
            return final_result
        except Exception as e:
            return self._log_err('get_realization', date, e)


    def get_realization_posting(self, date):
        try:
            real_date = datetime.strptime(date, "%Y-%m-%d")
            last_month_date = real_date - relativedelta(months=1)
            previous_month = last_month_date.month
            previous_year = last_month_date.year
            yyyy_mm = f"{previous_year}-{str(previous_month).zfill(2)}-01"
            resp = self.api._request('POST', '/v1/finance/realization/posting', json={
                'month': previous_month,
                'year': previous_year
            })
            result = resp.get('rows', [])
            for row in result:
                row['year_month'] = yyyy_mm
            final_result = self.common.spread_table(result)
            self._log_ok('get_realization_posting', date)
            return final_result
        except Exception as e:
            return self._log_err('get_realization_posting', date, e)

    def get_postings_fbo(self, date):
        try:
            all_postings = []
            offset = 0
            limit = 1000
            while True:
                data = self.api._request('POST', '/v2/posting/fbo/list', json={
                    'dir': 'ASC',
                    'filter': {
                        'since': f'{date}T00:00:00.000Z',
                        'status': '',
                        'to': f'{date}T23:59:59.999Z'
                    },
                    'limit': limit,
                    'offset': offset,
                    'with': {
                        'analytics_data': True,
                        'financial_data': True
                    }
                })
                result = data.get('result', [])
                if not result:
                    break
                all_postings.extend(result)
                offset += limit
            for item in all_postings:
                item['date'] = date
            self._log_ok('get_postings_fbo', date)
            return self.common.spread_table(all_postings)
        except Exception as e:
            return self._log_err('get_postings_fbo', date, e)

    def get_date_range(self, date):
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            day = date_obj.day
            if day <= 15:
                last_day_of_prev_month = date_obj.replace(day=1) - timedelta(days=1)
                start_date = last_day_of_prev_month.replace(day=16)
                end_date = last_day_of_prev_month
            else:
                start_date = date_obj.replace(day=1)
                end_date = date_obj.replace(day=15)
            return start_date.strftime("%Y-%m-%dT00:00:00.000Z"), end_date.strftime("%Y-%m-%dT23:59:59.999Z")
        except Exception as e:
            return self._log_err('get_date_range', date, e)


    def get_finance_total_pages(self, start_date, end_date):
        try:
            data = self.api._request('POST', '/v1/finance/cash-flow-statement/list', json={
                'date': {'from': start_date, 'to': end_date},
                'with_details': True,
                'page': 1,
                'page_size': 1
            })
            return data.get('page_count', 1)
        except Exception as e:
            return self._log_err('get_finance_total_pages', f'{start_date}-{end_date}', e)


    def get_finance_details(self, date):
        try:
            start_date, end_date = self.get_date_range(date)
            total_pages = self.get_finance_total_pages(start_date, end_date)
            all_details = []
            for page in range(1, total_pages + 1):
                data = self.api._request('POST', '/v1/finance/cash-flow-statement/list', json={
                    'date': {'from': start_date, 'to': end_date},
                    'with_details': True,
                    'page': page,
                    'page_size': 1000
                })
                result = data.get('result', {}).get('details', [])
                all_details.extend(result)
            self._log_ok('get_finance_details', date)
            return self.common.spread_table(all_details)
        except Exception as e:
            return self._log_err('get_finance_details', date, e)

    def get_finance_cashflow(self, date):
        try:
            start_date, end_date = self.get_date_range(date)
            total_pages = self.get_finance_total_pages(start_date, end_date)
            all_cashflows = []
            for page in range(1, total_pages + 1):
                data = self.api._request('POST', '/v1/finance/cash-flow-statement/list', json={
                    'date': {'from': start_date, 'to': end_date},
                    'with_details': True,
                    'page': page,
                    'page_size': 1000
                })
                result = data.get('result', {}).get('cash_flows', [])
                all_cashflows.extend(result)
            self._log_ok('get_finance_cashflow', date)
            return self.common.spread_table(all_cashflows)
        except Exception as e:
            return self._log_err('get_finance_cashflow', date, e)

    def get_products_buyout(self, date):
        try:
            resp = self.api._request('POST', '/v1/finance/products/buyout', json={
                'date_from': date,
                'date_to': date
            })
            result = resp.get('products', [])
            self._log_ok('get_products_buyout', date)
            return result
        except Exception as e:
            return self._log_err('get_products_buyout', date, e)


    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            if report == 'reklama':
                self.reklama = OZONreklama(self.bot_token, self.chat_list, self.message_type, self.subd, self.add_name, self.clientid, self.token,
                                           self.host, self.port, self.username, self.password,                                              self.database, self.start,  self.backfill_days)
                self.reklama.ozon_reklama_collector()
            else:
                self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port, self.username, self.password,
                                             self.database, self.start, self.add_name, self.err429, self.backfill_days, self.platform)
                self.clickhouse.collecting_report(
                    self.source_dict[report]['platform'],
                    self.source_dict[report]['report_name'],
                    self.source_dict[report]['upload_table'],
                    self.source_dict[report]['func_name'],
                    self.source_dict[report]['uniq_columns'],
                    self.source_dict[report]['partitions'],
                    self.source_dict[report]['merge_type'],
                    self.source_dict[report]['refresh_type'],
                    self.source_dict[report]['history'],
                    self.source_dict[report]['frequency'],
                    self.source_dict[report]['delay']
                )
        self.common.send_logs_clear_anyway(self.bot_token, self.chat_list)
