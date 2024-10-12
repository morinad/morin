from .common import Common
from .clickhouse import Clickhouse
import requests
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
import os
from dateutil import parser
import time
import logging
import hashlib
from io import StringIO
import json
from dateutil.relativedelta import relativedelta


class OZONbyDate:
    def __init__(self, logging_path:str, subd: str, add_name: str, clientid:str, token: str , host: str, port: str, username: str, password: str, database: str, start: str, backfill_days: int, reports :str):
        self.logging_path = logging_path
        self.clientid = clientid
        self.token = token
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.subd = subd
        self.add_name = add_name.replace(' ','').replace('-','_')
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.common = Common(logging_path)
        self.err429 = False

        logging.basicConfig(filename=logging_path,level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.source_dict = {
            'transactions': {
                'platform': 'ozon',
                'report_name': 'transactions',
                'func_name': self.get_transactions,
                'uniq_columns': 'operation_date, operation_id',
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
                'func_name': self.get_stock_on_warehouses,
                'uniq_columns': 'sku',
                'partitions': 'warehouse_name',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'products': {
                'platform': 'ozon',
                'report_name': 'products',
                'func_name': self.get_all_products,
                'uniq_columns': 'product_id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'returns_fbo': {
                'platform': 'ozon',
                'report_name': 'returns_fbo',
                'func_name': self.get_all_returns_fbo,
                'uniq_columns': 'return_id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'returns_fbs': {
                'platform': 'ozon',
                'report_name': 'returns_fbs',
                'func_name': self.get_all_returns_fbs,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 30
            },
            'realization': {
                'platform': 'ozon',
                'report_name': 'realization',
                'func_name': self.get_realization,
                'uniq_columns': 'year_month,rowNumber',
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
                'func_name': self.get_postings_fbo,
                'uniq_columns': 'posting_number,created_at',
                'partitions': 'date',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2,Friday'
                'delay': 30
            },
            'finance_details': {
                'platform': 'ozon',
                'report_name': 'finance_details',
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
                'func_name': self.get_finance_cashflow,
                'uniq_columns': 'period_id',
                'partitions': 'period_id',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': '1,16',  # '2,Friday'
                'delay': 30
            },
        }


    def get_transaction_page_count(self, date):
        try:
            url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
            headers = {"Client-Id": self.clientid,
                       "Api-Key": self.token,
                       "Content-Type": "application/json"}
            payload = {
                "filter": {
                    "date": {"from": f"{date}T00:00:00.000Z",
                             "to": f"{date}T23:59:59.999Z"},
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all"},
                "page": 1,
                "page_size": 1000
            }
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            code = response.status_code
            if code == 200:
                page_count = response.json()['result']['page_count']
                return page_count
            elif code == 429:
                self.err429 = True
            else:
                response.raise_for_status()
        except Exception as e:
            print(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            logging.info(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            return e

    def get_transactions(self, date):
        try:
            url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
            headers = {"Client-Id": self.clientid,
                       "Api-Key": self.token,
                       "Content-Type": "application/json"}
            page_count = int(self.get_transaction_page_count(date))
            operations = []
            for page in range(1, page_count + 1):
                print(page)
                payload = {
                    "filter": {
                        "date": {"from": f"{date}T00:00:00.000Z",
                                 "to": f"{date}T23:59:59.999Z"},
                        "operation_type": [],
                        "posting_number": "",
                        "transaction_type": "all"},
                    "page": page,
                    "page_size": 1000
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 200:
                    operations += response.json()['result']['operations']
                    return operations
                elif code == 429:
                    self.err429 = True
                else:
                    response.raise_for_status()
                time.sleep(2)
        except Exception as e:
            print(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            logging.info(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            raise

    def get_stock_on_warehouses(self, date):
        try:
            url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            offset = 0
            limit = 1000
            all_rows = []  # Список для хранения всех записей из 'rows'

            while True:
                payload = {
                    "limit": limit,
                    "offset": offset,
                    "warehouse_type": "ALL"
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code

                if code == 200:
                    rows = response.json().get('result', {}).get('rows', [])
                    if not rows:
                        # Прекращаем цикл, если ответ пустой
                        break
                    all_rows.extend(rows)  # Добавляем все элементы 'rows' в общий список
                    offset += limit
                elif code == 429:
                    self.err429 = True
                    break
                else:
                    response.raise_for_status()
            return all_rows  # Возвращаем итоговый список из 'rows'
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - stocks.')
            logging.info(f'Ошибка: {e}. Запрос - stocks.')
            return e

    def get_all_products(self, date):
        try:
            url = "https://api-seller.ozon.ru/v2/product/list"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            limit = 1000
            last_id = ""  # Инициализируем last_id пустым значением для первого запроса
            all_items = []  # Список для хранения всех продуктов
            while True:
                payload = {
                    "last_id": last_id,
                    "limit": limit
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 200:
                    result = response.json().get('result', {})
                    items = result.get('items', [])
                    if not items:
                        break
                    all_items.extend(items)
                    if len(items) < limit:
                        break
                    last_id = result.get('last_id', "")
                elif code == 429:
                    self.err429 = True
                    break
                else:
                    response.raise_for_status()
            return all_items  # Возвращаем итоговый список из 'items'
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - products.')
            logging.info(f'Ошибка: {e}. Запрос - products.')
            return e

    def get_all_returns_fbo(self, date):
        try:
            url = "https://api-seller.ozon.ru/v3/returns/company/fbo"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            limit = 1000  # Можно задать желаемый лимит записей на один запрос
            last_id = 0  # Инициализируем last_id с начальным значением 0
            all_returns = []  # Список для хранения всех возвратов
            while True:
                payload = {
                    "last_id": last_id,
                    "limit": limit
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 200:
                    result = response.json()
                    returns = result.get('returns', [])
                    if not returns:
                        break
                    all_returns.extend(returns)  # Добавляем все элементы 'returns' в общий список
                    if len(returns) < limit:
                        break
                    last_id = result.get('last_id', 0)
                elif code == 429:
                    self.err429 = True
                    break
                else:
                    response.raise_for_status()
            return all_returns  # Возвращаем итоговый список из 'returns'
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - returns_fbo.')
            logging.info(f'Ошибка: {e}. Запрос - returns_fbo.')
            return e

    def get_all_returns_fbs(self, date):
        try:
            url = "https://api-seller.ozon.ru/v3/returns/company/fbs"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            limit = 1000  # Устанавливаем лимит на количество записей за запрос
            last_id = 0  # Инициализируем last_id с начальным значением 0
            all_returns = []  # Список для хранения всех возвратов
            while True:
                payload = {
                    "limit": limit,
                    "last_id": last_id
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 200:
                    result = response.json()
                    returns = result.get('returns', [])
                    if not returns:
                        break
                    all_returns.extend(returns)  # Добавляем все элементы 'returns' в общий список
                    if len(returns) < limit:
                        break
                    last_id = result.get('last_id', 0)
                elif code == 429:
                    self.err429 = True
                    break
                else:
                    response.raise_for_status()
            return all_returns  # Возвращаем итоговый список из 'returns'
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - returns_fbs.')
            logging.info(f'Ошибка: {e}. Запрос - returns_fbs.')
            return e

    def get_realization(self, date):
        real_date = datetime.strptime(date, "%Y-%m-%d")
        last_month_date = real_date - relativedelta(months=1)
        previous_month = last_month_date.month
        previous_year = last_month_date.year
        yyyy_mm = f"{previous_year}-{str(previous_month).zfill(2)}-01"
        final_data = []
        data = {
            "month": previous_month,
            "year": previous_year
        }
        headers = {
            "Client-Id": self.clientid,
            "Api-Key": self.token,
            "Content-Type": "application/json"
        }
        url = "https://api-seller.ozon.ru/v2/finance/realization"
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            result = response.json().get('result', {}).get('rows', [])
            for row in result:
                row['year_month'] = yyyy_mm
                final_data.append(row)
            return self.common.spread_table(final_data)
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def get_postings_fbo(self, date):
        try:
            url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            offset = 0
            limit = 1000
            all_postings = []  # Список для хранения всех отправлений
            while True:
                payload = {
                    "dir": "ASC",
                    "filter": {
                        "since": f"{date}T00:00:00.000Z",  # Дата с началом дня
                        "status": "",
                        "to": f"{date}T23:59:59.999Z"  # Дата с концом дня
                    },
                    "limit": limit,
                    "offset": offset,
                    "with": {
                        "analytics_data": True,
                        "financial_data": True
                    }
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 200:
                    result = response.json().get('result', [])
                    if not result:
                        break
                    all_postings.extend(result)
                    offset += limit
                elif code == 429:
                    self.err429 = True  # Устанавливаем флаг ошибки 429
                    break
                else:
                    response.raise_for_status()
            all_postings_with_date = []
            for dict in all_postings:
                dict['date']=date
                all_postings_with_date.append(dict)
            return self.common.spread_table(all_postings_with_date)  # Возвращаем итоговый список отправлений
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - postings.')
            logging.info(f'Ошибка: {e}. Запрос - postings.')
            return e

    def get_date_range(self, date):
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        day = date_obj.day
        if day == 1:
            # С 16-го числа предыдущего месяца по конец предыдущего месяца
            last_day_of_prev_month = date_obj.replace(day=1) - timedelta(days=1)
            start_date = last_day_of_prev_month.replace(day=16)
            end_date = last_day_of_prev_month
        else:
            # С 1-го по 15-е число текущего месяца
            start_date = date_obj.replace(day=1)
            end_date = date_obj.replace(day=15)
        return start_date.strftime("%Y-%m-%dT00:00:00.000Z"), end_date.strftime("%Y-%m-%dT23:59:59.999Z")

    def get_finance_total_pages(self, start_date, end_date):
        """Определяет общее количество страниц"""
        url = "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list"
        headers = {
            "Client-Id": self.clientid,
            "Api-Key": self.token,
            "Content-Type": "application/json"
        }
        payload = {
            "date": {
                "from": start_date,
                "to": end_date
            },
            "with_details": True,
            "page": 1,
            "page_size": 1
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            return response.json().get('page_count', 1)
        else:
            response.raise_for_status()

    def get_finance_details(self, date):
        try:
            start_date, end_date = self.get_date_range(date)
            total_pages = self.get_finance_total_pages(start_date, end_date)
            url = "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            all = []  # Список для хранения всех cash_flows
            for page in range(1, total_pages + 1):
                payload = {
                    "date": {
                        "from": start_date,
                        "to": end_date
                    },
                    "with_details": True,
                    "page": page,
                    "page_size": 1000
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                if response.status_code == 200:
                    result = response.json().get('result', {}).get('details', [])
                    all.extend(result)
                else:
                    response.raise_for_status()
            return self.common.spread_table(all)
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - finance_details.')
            logging.info(f'Ошибка: {e}. Запрос - finance_details.')
            return e

    def get_finance_cashflow(self, date):
        try:
            start_date, end_date = self.get_date_range(date)
            total_pages = self.get_finance_total_pages(start_date, end_date)
            url = "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list"
            headers = {
                "Client-Id": self.clientid,
                "Api-Key": self.token,
                "Content-Type": "application/json"
            }
            all = []  # Список для хранения всех cash_flows
            for page in range(1, total_pages + 1):
                payload = {
                    "date": {
                        "from": start_date,
                        "to": end_date
                    },
                    "with_details": True,
                    "page": page,
                    "page_size": 1000
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                if response.status_code == 200:
                    result = response.json().get('result', {}).get('cash_flows', [])
                    all.extend(result)
                else:
                    response.raise_for_status()
            return self.common.spread_table(all)
        except Exception as e:
            print(f'Ошибка: {e}. Запрос - finance_cashflow.')
            logging.info(f'Ошибка: {e}. Запрос - finance_cashflow.')
            return e


    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            self.clickhouse = Clickhouse(self.logging_path, self.host, self.port, self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days)
            self.clickhouse.collecting_report(
                self.source_dict[report]['platform'],
                self.source_dict[report]['report_name'],
                self.source_dict[report]['func_name'],
                self.source_dict[report]['uniq_columns'],
                self.source_dict[report]['partitions'],
                self.source_dict[report]['merge_type'],
                self.source_dict[report]['refresh_type'],
                self.source_dict[report]['history'],
                self.source_dict[report]['frequency'],
                self.source_dict[report]['delay']
            )


