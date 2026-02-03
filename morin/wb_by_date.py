from .common import Common
from .clickhouse import Clickhouse
from .wb_reklama import WBreklama
import requests
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
import os
from dateutil import parser
import time
import hashlib
from io import StringIO
import json


class WBbyDate:
    def __init__(self, bot_token:str = '', chats:str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', token: str  = '',  start: str = '', backfill_days: int = 0, reports :str = ''):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
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
        self.yesterday = self.today - timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%Y-%m-%d")
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.platform = 'wb'
        self.err429 = False
        self.source_dict = {
            'realized': {
                'platform': 'wb',
                'report_name': 'realized',
                'upload_table': 'realized',
                'func_name': self.get_realized,
                'uniq_columns': 'realizationreport_id,rrd_id',
                'partitions': 'realizationreport_id',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'Monday',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'orders': {
                'platform': 'wb',
                'report_name': 'orders',
                'upload_table': 'orders',
                'func_name': self.get_orders,
                'uniq_columns': 'date,srid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'sbor_orders': {
                'platform': 'wb',
                'report_name': 'sbor_orders',
                'upload_table': 'sbor_orders',
                'func_name': self.get_sbor,
                'uniq_columns': 'id,rid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
            'sbor_status': {
                'platform': 'wb',
                'report_name': 'sbor_status',
                'upload_table': 'sbor_status',
                'func_name': self.get_sbor_status,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
            'incomes': {
                'platform': 'wb',
                'report_name': 'incomes',
                'upload_table': 'incomes',
                'func_name': self.get_incomes,
                'uniq_columns': 'incomeId,barcode',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'excise': {
                'platform': 'wb',
                'report_name': 'excise',
                'upload_table': 'excise',
                'func_name': self.get_excise,
                'uniq_columns': 'fiscal_dt,nm_id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'sales': {
                'platform': 'wb',
                'report_name': 'sales',
                'upload_table': 'sales',
                'func_name': self.get_sales,
                'uniq_columns': 'date,saleID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'orders_changes': {
                'platform': 'wb',
                'report_name': 'orders_changes',
                'upload_table': 'orders',
                'func_name': self.get_orders_changes,
                'uniq_columns': 'date,srid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'sales_changes': {
                'platform': 'wb',
                'report_name': 'sales_changes',
                'upload_table': 'sales',
                'func_name': self.get_sales_changes,
                'uniq_columns': 'date,saleID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'stocks': {
                'platform': 'wb',
                'report_name': 'stocks',
                'upload_table': 'stocks',
                'func_name': self.get_stocks,
                'uniq_columns': 'lastChangeDate',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'cards': {
                'platform': 'wb',
                'report_name': 'cards',
                'upload_table': 'cards',
                'func_name': self.get_cards,
                'uniq_columns': 'nmID',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'stocks_history': {
                'platform': 'wb',
                'report_name': 'stocks_history',
                'upload_table': 'stocks_history',
                'func_name': self.get_stocks,
                'uniq_columns': 'lastChangeDate',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'adv_upd': {
                'platform': 'wb',
                'report_name': 'adv_upd',
                'upload_table': 'adv_upd',
                'func_name': self.get_adv_upd,
                'uniq_columns': 'advertId,updTime,paymentType',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 5
            },
            'paid_storage': {
                'platform': 'wb',
                'report_name': 'paid_storage',
                'upload_table': 'paid_storage',
                'func_name': self.get_paid_storage,
                'uniq_columns': 'date',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
            'voronka_week': {
                'platform': 'wb',
                'report_name': 'voronka_week',
                'upload_table': 'voronka_week',
                'func_name': self.get_voronka_week,
                'uniq_columns': 'nmId,date',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 23
            },
            'voronka_all': {
                'platform': 'wb',
                'report_name': 'voronka_all',
                'upload_table': 'voronka_all',
                'func_name': self.get_voronka_all,
                'uniq_columns': 'product_nmId,statistic_selected_period_start',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 23
            },
            'feedbacks': {
                'platform': 'wb',
                'report_name': 'feedbacks',
                'upload_table': 'feedbacks',
                'func_name': self.get_feedbacks,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
        }

    def get_adv_upd(self, date):
        try:
            url = "https://advert-api.wildberries.ru/adv/v1/upd"
            headers = {"Authorization": self.token}
            params = {"from": date, "to": date }
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_adv_upd. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
                message = f'Платформа: WB. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_adv_upd. Ошибка: {e}.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                return message

    def get_sbor_status(self, date):
        try:
            url = "https://marketplace-api.wildberries.ru/api/v3/orders"
            headers = {"Authorization": self.token}
            next = '0'
            final_result = []
            while True:
                ids_to_collect = []
                params = {'limit': "1000", 'next': next,
                          "dateFrom": int(datetime.strptime( date+' 00:00:00', "%Y-%m-%d %H:%M:%S").timestamp()),
                          "dateTo" : int(datetime.strptime( date+' 23:59:59', "%Y-%m-%d %H:%M:%S").timestamp())}
                response = requests.get(url, headers=headers, params=params)
                code = response.status_code
                if code == 429:
                    self.err429 = True
                if code == 200:
                    next = str(response.json()['next'])
                    orders = response.json()['orders']
                    if len(orders)==0:
                        break
                    else:
                        for i in orders:
                            ids_to_collect.append(i['id'])
                else:
                    response.raise_for_status()
                status_url = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
                status_headers = {"Authorization": self.token, 'Content-Type': 'application/json'}
                payload = {"orders": ids_to_collect}
                response = requests.post(status_url, headers=status_headers, json=payload)
                code = response.status_code
                if code == 429:
                    self.err429 = True
                if code == 200:
                    data = response.json()['orders']
                    final_result += data
                else:
                    response.raise_for_status()
                if len(orders)<1000:
                    break
                time.sleep(1)
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sbor_status. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
                message = f'Платформа: WB. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_sbor_status. Ошибка: {e}.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                return message


    def get_sbor(self, date):
        try:
            url = "https://marketplace-api.wildberries.ru/api/v3/orders"
            headers = {"Authorization": self.token}
            next = '0'
            final_result = []
            while True:
                params = {'limit': "1000", 'next': next,
                          "dateFrom": int(datetime.strptime( date+' 00:00:00', "%Y-%m-%d %H:%M:%S").timestamp()),
                          "dateTo" : int(datetime.strptime( date+' 23:59:59', "%Y-%m-%d %H:%M:%S").timestamp())}
                response = requests.get(url, headers=headers, params=params)
                code = response.status_code
                if code == 429:
                    self.err429 = True
                if code == 200:
                    orders = response.json()['orders']
                    if len(orders)==0:
                        break
                    else:
                        final_result += orders
                else:
                    response.raise_for_status()
                next = str(response.json()['next'])
                if len(orders)<1000:
                    break
                time.sleep(1)
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sbor. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
                message = f'Платформа: WB. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_sbor. Ошибка: {e}.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                return message

    def create_ps_report(self, api_key, date1, date2):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
            headers = {"Authorization": api_key}
            params = {"dateFrom": date1, "dateTo": date2, }
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 200:
                return response.json()['data']['taskId']
            else:
                response.raise_for_status()
        except Exception as e:
                message = f'Платформа: WB. Имя: {self.add_name}. Даты: {date1}-{date2}. Функция: create_ps_report. Ошибка: {e}.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                return message


    def ps_report_status(self, api_key, task_id):
        try:
            url = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status"
            headers = {"Authorization": api_key}
            response = requests.get(url, headers=headers)
            code = response.status_code
            if code == 200:
                return response.json()['data']['status']
            else:
                response.raise_for_status()
        except Exception as e:
                message = f'Платформа: WB. Имя: {self.add_name}. Функция: ps_report_status. Ошибка: {e}.'
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
                return message


    def get_ps_report(self, api_key, task_id):
        try:
            url = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download"
            headers = {"Authorization": api_key}
            response = requests.get(url, headers=headers)
            code = response.status_code
            if code == 200:
                return response.json()
            else:
                response.raise_for_status()
        except Exception as e:
                message = f'Платформа: WB. Имя: {self.add_name}. Функция: get_ps_report. Ошибка: {e}.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                return message


    def get_paid_storage(self, date):
        try:
            task = self.create_ps_report(self.token, date, date)
            for t in range(20):
                time.sleep(10)
                if self.ps_report_status(self.token, task) =='done':
                    message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_paid_storage. Результат: ОК'
                    self.common.log_func(self.bot_token, self.chat_list, message, 1)
                    return self.get_ps_report(self.token, task)
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_paid_storage. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    # дата+токен -> список словарей с заказами (данные)
    def get_orders(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            headers = {
                "Authorization": self.token,
            }
            params = {
                "dateFrom": date_rfc3339,
                "flag": 1,  # Для получения всех заказов на указанную дату
            }
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            print(code)
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()

            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_orders. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_orders. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_incomes(self, date=''):
        try:
            date_rfc3339 = f"{self.start}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"
            headers = {
                "Authorization": self.token,
            }
            params = {
                "dateFrom": date_rfc3339
            }
            response = requests.get(url, headers=headers, params=params, timeout=200)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                json_data = response.json()
                if not json_data or all(not item for item in json_data if isinstance(json_data, list)):
                    raise ValueError("Получен пустой Json")
                final_result = json_data
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_incomes. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_incomes. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_orders_changes(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            headers = {"Authorization": self.token}
            params = {"dateFrom": date_rfc3339}
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_orders_changes. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_orders_changes. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    # дата+токен -> список словарей с заказами (данные)
    def get_sales(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            headers = {
                'Authorization': f'Bearer {self.token}'
            }
            params = {
                'dateFrom': date,
                "flag": 1,
            }
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sales. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sales. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_excise(self, date):
        try:
            url = 'https://seller-analytics-api.wildberries.ru/api/v1/analytics/excise-report'
            headers = {'Authorization': f'{self.token}'  , "Content-Type" : "application/json"}
            jsondata = json.dumps({}, ensure_ascii=False).encode("utf8")
            params = {                'dateFrom': self.start,
                'dateTo': self.yesterday_str            }
            response = requests.post(url, headers=headers, params=params,data = jsondata)
            print(response.json())
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()['response']['data']
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_excise. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_excise. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_sales_changes(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            headers = {'Authorization': f'Bearer {self.token}'}
            params = {'dateFrom': date}
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sales_changes. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sales_changes. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    # дата+токен -> список словарей с заказами (данные)
    def get_realized(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'
            headers = {'Authorization': f'Bearer {self.token}'}
            params = {'dateFrom': self.common.shift_date(date,7), 'dateTo': self.common.shift_date(date,1)}
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_realized. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_realized. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_stocks(self, date=''):
        try:
            # Преобразуем дату в формат RFC3339
            date_rfc3339 = f"{self.start}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
            headers = {
                "Authorization": self.token,
            }
            params = {
                "dateFrom": date_rfc3339,
            }
            response = requests.get(url, headers=headers, params=params)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                final_result = response.json()
            else:
                response.raise_for_status()
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_stocks. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return     final_result
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_stocks. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_voronka_week(self, date):
        try:
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
            nm_list_raw = self.clickhouse.get_table_data(f'{self.platform}_cards_{self.add_name}', ' nmID ')
            nm_list = [row['nmID'] for row in nm_list_raw] if nm_list_raw else []
            final_list = self.common.get_chunks(nm_list,20)
            url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products/history"
            headers = {
                "Authorization": self.token,
                "Content-Type": "application/json"
            }
            all_cards = []
            for chunk in final_list:
                payload = {
                        "selectedPeriod": {
                            "start": f"{date}",
                            "end": f"{date}"
                        },
                    "nmIds": chunk,
                        "skipDeletedNm": True,
                        "aggregationLevel": "day"
                    }
                response = requests.post(url, headers=headers, json=payload)
                code = response.status_code
                if code == 200:
                    data = response.json()
                    for card in data:
                        if len(card['history']) == 0:
                            pass
                        elif len(card['history'])>1:
                            response.raise_for_status()
                        else:
                            card_dict = card['product'] | card['history'][0]
                            all_cards.append(card_dict)
                else:
                    response.raise_for_status()
                time.sleep(23)
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_voronka_week. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return self.common.spread_table(self.common.spread_table(self.common.spread_table(all_cards)))
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_voronka_week. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_voronka_all(self, date):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
            headers = {
                "Authorization": self.token,
                "Content-Type": "application/json"
            }
            offset = 0
            limit = 1000
            all_cards = []
            while True:
                payload = {
                        "selectedPeriod": {
                            "start": f"{date}",
                            "end": f"{date}"
                        },
                        "skipDeletedNm": True,
                    "limit" : limit,
                    "offset" : offset
                    }
                response = requests.post(url, headers=headers, json=payload)

                code = response.status_code
                if code == 200:
                    data = response.json()['data']['products']
                    print(len(data))
                    all_cards.extend(data)
                    if len(data)<limit:
                        break
                    else:
                        offset += limit
                else:
                    response.raise_for_status()
                time.sleep(23)
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_voronka_all. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return self.common.spread_table(self.common.spread_table(self.common.spread_table(all_cards)))
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_voronka_all. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_cards(self, date=''):
        try:
            url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
            headers = {
                "Authorization": self.token,
                "Content-Type": "application/json"
            }

            all_cards = []  # Хранилище для всех карточек товара
            cursor = {"limit": 100}  # Начальный курсор

            while True:
                payload = {
                    "settings": {
                        "cursor": cursor,
                        "filter": {
                            "withPhoto": -1
                        }
                    }
                }

                response = requests.post(url, headers=headers, json=payload)
                code = response.status_code

                if code == 429:
                    self.err429 = True
                    time.sleep(60)  # Ждем минуту при превышении лимита
                    continue

                if code == 200:
                    data = response.json()
                    cards = data.get('cards', [])
                    cursor_info = data.get('cursor', {})

                    if not cards:  # Если карточек нет, выходим
                        break

                    all_cards.extend(cards)  # Добавляем карточки на текущей странице

                    # Проверяем, есть ли еще данные
                    total = cursor_info.get('total', 0)
                    if total < 100:  # Если получили меньше лимита, это последняя страница
                        break

                    # Обновляем курсор для следующего запроса
                    if 'updatedAt' in cursor_info and 'nmID' in cursor_info:
                        cursor = {
                            "limit": 100,
                            "updatedAt": cursor_info['updatedAt'],
                            "nmID": cursor_info['nmID']
                        }
                    else:
                        break  # Если нет данных для курсора, выходим

                    time.sleep(2)  # Пауза между запросами для соблюдения лимитов

                else:
                    response.raise_for_status()

            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_cards. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return self.common.spread_table(self.common.spread_table(all_cards))

        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_cards. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_chosen_feedbacks(self, date, answered):
        try:
            take = 5000
            url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
            headers = {"Authorization": self.token}
            all_feedbacks = []
            skip = 0
            while True:
                params = {'order': 'dateAsc', 'isAnswered': answered, 'take': str(take), 'skip': str(skip), "dateFrom": str(self.common.datetime_to_unixtime(date +' 00:00:00')), "dateTo": str(self.common.datetime_to_unixtime(date+ ' 23:59:59'))}
                response = requests.get(url, headers=headers, params=params)
                code = response.status_code
                if code == 429:
                    self.err429 = True
                if code == 200:
                    result = response.json()['data']['feedbacks']
                    if len(result) == 0:
                        break
                    else:
                        all_feedbacks.extend(result)
                else:
                    response.raise_for_status()
                skip = skip+ take
                time.sleep(2)
            return all_feedbacks
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_chosen_feedbacks. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_feedbacks(self, date):
        try:
            all_feedbacks = []
            all_feedbacks.extend(self.get_chosen_feedbacks(date, "true"))
            all_feedbacks.extend(self.get_chosen_feedbacks(date, "false"))
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_feedbacks. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return self.common.spread_table(self.common.spread_table(all_feedbacks))
        except Exception as e:
            message = f'Платформа: WB. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_feedbacks. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    # тип отчёта, дата -> данные в CH
    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            if report == 'reklama':
                self.reklama = WBreklama(self.bot_token, self.chat_list, self.message_type, self.subd, self.add_name, self.token, self.host, self.port, self.username, self.password,
                                             self.database, self.start,  self.backfill_days,)
                self.reklama.wb_reklama_collector()
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









