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
    def __init__(self, bot_token:str, chats:str, message_type: str, subd: str,
                 host: str, port: str, username: str, password: str, database: str,
                 add_name: str, token: str ,  start: str, backfill_days: int, reports :str):
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
            'nmreport': {
                'platform': 'wb',
                'report_name': 'nmreport',
                'upload_table': 'nmreport',
                'func_name': self.get_nmreport,
                'uniq_columns': 'nmID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 60
            },
        }

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
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            message = f'Код: {code}, запрос - orders'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Ошибка: {e}, запрос - orders'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

    def get_orders_changes(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            headers = {"Authorization": self.token}
            params = {"dateFrom": date_rfc3339}
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            message = f'Код: {code}, запрос - orders_changes'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Ошибка: {e}, запрос - orders_changes'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

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
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            message = f'Код: {code}, запрос - sales'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Ошибка: {e}, запрос - sales'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

    def get_sales_changes(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            headers = {'Authorization': f'Bearer {self.token}'}
            params = {'dateFrom': date}
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            message = f'Код: {code}, запрос - sales_changes'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Ошибка: {e}, запрос - sales_changes'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

    # дата+токен -> список словарей с заказами (данные)
    def get_realized(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'
            headers = {'Authorization': f'Bearer {self.token}'}
            params = {'dateFrom': self.common.shift_date(date,8), 'dateTo': self.common.shift_date(date,1)}
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            message = f'Код: {code}, запрос - realized'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Ошибка: {e}, запрос - realized'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

    def get_stocks(self, date):
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
            code = str(response.status_code)
            if code == '200':
                return response.json()  # Возвращаем данные при успешном ответе
            elif code == '429':
                self.err429 = True  # Фиксируем ошибку 429 (превышение лимита запросов)
            else:
                response.raise_for_status()  # Поднимаем исключение для других кодов
        except Exception as e:
            message = f'Ошибка: {e}, запрос - stocks'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

    def get_nmreport(self, date):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
            headers = {
                "Authorization": self.token,
                "Content-Type": "application/json"
            }
            page = 1
            all_cards = []  # Хранилище для всех карточек товара
            begin_date = f"{date} 00:00:00"
            end_date = f"{date} 23:59:59"
            while True:
                payload = {
                    "period": {
                        "begin": begin_date,
                        "end": end_date
                    },
                    "page": page
                }
                response = requests.post(url, headers=headers, json=payload)
                code = response.status_code
                if code == 200:
                    data = response.json().get('data', {})
                    cards = data.get('cards', [])
                    all_cards.extend(cards)  # Добавляем карточки на текущей странице
                    is_next_page = data.get('isNextPage', False)
                    if not is_next_page:
                        break  # Если страниц больше нет, выходим из цикла
                    page += 1  # Переходим на следующую страницу
                else:
                    response.raise_for_status()
            return self.common.spread_table(self.common.spread_table(self.common.spread_table(all_cards)))
        except Exception as e:
            message = f'Ошибка: {e}. Дата: {date}. Запрос - nm_report.'
            print(message)
            self.common.send_log_message(self.bot_token, self.chat_list, message, 3)
            return e

    # тип отчёта, дата -> данные в CH
    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            if report == 'reklama':
                self.reklama = WBreklama(self.bot_token, self.chat_list, self.message_type, self.subd, self.add_name, self.token, self.host, self.port, self.username, self.password,
                                             self.database, self.start,  self.backfill_days)
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








