from .common import Common
from .clickhouse import Clickhouse
from .ozon_reklama import OZONreklama
import requests
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
from transliterate import translit
import os
from dateutil import parser
import time
import hashlib
from io import StringIO
import json
from dateutil.relativedelta import relativedelta


class GCbyDate:
    def __init__(self, bot_token:str = '', chats:str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', clientid:str = '', token: str  = '', start: str = '', group_id: str = '', reports :str = ''):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ','').split(',')
        self.message_type  = message_type
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
        self.group_id = group_id
        self.backfill_days = 3
        self.platform = 'gc'
        self.err429 = False
        self.source_dict = {
            'users': {
                'platform': 'gc',
                'report_name': 'users',
                'upload_table': 'users',
                'func_name': self.get_users,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily', # '2dayOfMonth,Friday'
                'delay': 30
            },
            'deals': {
                'platform': 'gc',
                'report_name': 'deals',
                'upload_table': 'deals',
                'func_name': self.get_deals,
                'uniq_columns': 'id_zakaza',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily', # '2dayOfMonth,Friday'
                'delay': 30
            },
            'payments': {
                'platform': 'gc',
                'report_name': 'payments',
                'upload_table': 'payments',
                'func_name': self.get_payments,
                'uniq_columns': 'nomer',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily', # '2dayOfMonth,Friday'
                'delay': 30
            },
            'groups': {
                'platform': 'gc',
                'report_name': 'groups',
                'upload_table': 'groups',
                'func_name': self.get_groups,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily', # '2dayOfMonth,Friday'
                'delay': 30
            }
        }

    def translate_keys(self, list_of_dicts):
        try:
            def transliterate_key(key):
                tr = translit(key, 'ru', reversed=True)
                return    tr.replace(' ','_').replace('?','').replace('-','_').replace(",",'').replace("'",'').replace(".",'').replace("(",'').replace(")",'').lower()
            for dictionary in list_of_dicts:
                new_dict = {}
                for key, value in dictionary.items():
                    # Транслитерируем ключ
                    english_key = transliterate_key(key)
                    new_dict[english_key] = value
                dictionary.clear()  # Очищаем оригинальный словарь
                dictionary.update(new_dict)  # Обновляем его новыми значениями с английскими ключами
            return list_of_dicts
        except Exception as e:
            message = f'Платформа: GC. Имя: {self.add_name}. Функция: translate_keys. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_data(self, report):
        try:
            delay = 10
            max_attempts = 20
            querydata = {"key": self.token}
            if report == "groups":
                report += r'/' +str(self.group_id) + r'/users'
            url = f"{self.clientid}/pl/api/account/{report}?created_at[from]={self.start}".replace(r'//pl',r'/pl')
            response = requests.get(url, params=querydata)
            code = response.status_code
            if code == 429:
                self.err429 = True
            if code == 200:
                time.sleep(delay)
                for attempt in range(max_attempts):
                    try:
                        export_id = response.json()["info"].get("export_id")
                        break
                    except:
                        message = f"Попытка {attempt + 1}: Ответ: {str(response.json())}"
                        self.common.log_func(self.bot_token, self.chat_list, message, 1)
                        time.sleep(delay)
                export_url = f"{self.clientid}/pl/api/account/exports/{export_id}".replace(r'//pl',r'/pl')
                for attempt in range(max_attempts):
                    time.sleep(delay)
                    export_response = requests.get(export_url, params=querydata)
                    export_code = export_response.status_code
                    if export_code == 429:
                        self.err429 = True
                    if export_code == 200:
                        json_data = export_response.json()
                        if json_data.get("success"):
                            items = json_data["info"].get("items", [])
                            fields = json_data["info"].get("fields", [])
                            result_data = [{fields[i]: item[i] for i in range(len(fields))} for item in items]
                            return self.translate_keys(result_data)
                    else:
                        message = f"Попытка {attempt + 1}: Статус {str(export_code)}"
                        self.common.log_func(self.bot_token, self.chat_list, message,1)
                return None
            else:
                response.raise_for_status()
        except Exception as e:
            message = f'Платформа: GC. Имя: {self.add_name}. Функция: get_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise


    def get_users(self, date=''):
        try:
            final_result = self.get_data('users')
            message = f'Платформа: GC. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_users. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: GC. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_users. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_deals(self, date=''):
        try:
            final_result = self.get_data('deals')
            message = f'Платформа: GC. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_deals. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: GC. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_deals. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_payments(self, date=''):
        try:
            final_result = self.get_data('payments')
            message = f'Платформа: GC. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_payments. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: GC. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_payments. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_groups(self, date=''):
        try:
            final_result = self.get_data('groups')
            message = f'Платформа: GC. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_groups. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: GC. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_groups. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
                self.clickhouse = Clickhouse( self.bot_token, self.chat_list, self.message_type, self.host, self.port, self.username, self.password,
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


