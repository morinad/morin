from .common import Common
from .clickhouse import Clickhouse
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


class VKbyDate:
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
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.platform = 'vk'
        self.err429 = False
        self.source_dict = {
            'banners_stat': {
                'platform': 'vk',
                'report_name': 'banners_stat',
                'upload_table': 'banners_stat',
                'func_name': self.get_banners_stat,
                'uniq_columns': 'id,date',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
                        'groups_stat': {
                'platform': 'vk',
                'report_name': 'groups_stat',
                'upload_table': 'groups_stat',
                'func_name': self.get_groups_stat,
                'uniq_columns': 'id,date',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
            'campaigns_stat': {
                'platform': 'vk',
                'report_name': 'campaigns_stat',
                'upload_table': 'campaigns_stat',
                'func_name': self.get_campaigns_stat,
                'uniq_columns': 'id,date',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
            'banners_list': {
                'platform': 'vk',
                'report_name': 'banners_list',
                'upload_table': 'banners_list',
                'func_name': self.get_banners_list,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
                        'groups_list': {
                'platform': 'vk',
                'report_name': 'groups_list',
                'upload_table': 'groups_list',
                'func_name': self.get_groups_list,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
            'campaigns_list': {
                'platform': 'vk',
                'report_name': 'campaigns_list',
                'upload_table': 'campaigns_list',
                'func_name': self.get_campaigns_list,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
        }

    def get_vk_statistics(self,endpoint, date1, date2, fields, offset):
        try:
            limit = 250
            base_url = f"https://ads.vk.com/api/v3/statistics/{endpoint}/day.json"
            params = {
                "date_from": date1,
                "date_to": date2,
                "fields": fields,
                "limit": limit,
                "offset": offset,
            }
            headers = {"Authorization": f"Bearer {self.token}"}
            response = requests.get(base_url, params=params, headers=headers)
            result = response.json()
            code = response.status_code
            if code != 200:
                response.raise_for_status()
            return result['items'], result['count']
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date1)}-{str(date2)}. Функция: get_vk_statistics. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def fetch_vk_stat(self,endpoint, date):
        try:
            all_data = []
            offset = 0
            fields = 'all'
            for q in range(10000):
                data, count = self.get_vk_statistics(endpoint, date, date, fields, offset)
                all_data += data
                if int(count) < 250:
                    break
                else:
                    offset += 250
            for row in all_data:
                row['date']=date
            return self.common.spread_table(self.common.spread_table(self.common.spread_table(self.common.spread_table(all_data))))
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date1)}-{str(date2)}. Функция: fetch_vk_stat. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_objects(self,report, offset):
        try:
            base_url = f"https://ads.vk.com/api/v2/{report}.json"
            params = {"limit": 50, "offset": offset}
            headers = {"Authorization": f"Bearer {self.token}"}
            response = requests.get(base_url, params=params, headers=headers)
            result = response.json()
            code = response.status_code
            if code != 200:
                response.raise_for_status()
            return result['items'], result['count']
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Функция: get_objects. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def fetch_vk_objects(self,report):
        try:
            all_objects = []
            offset = 0
            for q in range(10000):
                data, count = self.get_objects(report, offset)
                all_objects += data
                if int(count) < 50:
                    break
                else:
                    offset += 50
            return all_objects
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Функция: fetch_vk_objects. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_banners_stat(self, date):
        try:
            final_result = self.fetch_vk_stat('banners', date)
            message = f'Платформа: VK. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_banners_stat. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_banners_stat. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message

    def get_campaigns_stat(self, date):
        try:
            final_result = self.fetch_vk_stat('ad_plans', date)
            message = f'Платформа: VK. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_campaigns_stat. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_campaigns_stat. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message

    def get_groups_stat(self, date):
        try:
            final_result = self.fetch_vk_stat('ad_groups', date)
            message = f'Платформа: VK. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_groups_stat. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_groups_stat. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message

    def get_banners_list(self, date=''):
        try:
            final_result = self.fetch_vk_objects('banners')
            message = f'Платформа: VK. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_banners_list. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_banners_list. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message

    def get_groups_list(self, date=''):
        try:
            final_result = self.fetch_vk_objects('ad_groups')
            message = f'Платформа: VK. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_groups_list. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_groups_list. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message

    def get_campaigns_list(self, date=''):
        try:
            final_result = self.fetch_vk_objects('ad_plans')
            message = f'Платформа: VK. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_campaigns_list. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: VK. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_campaigns_list. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message

    # тип отчёта, дата -> данные в CH
    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
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









