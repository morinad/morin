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
import math


class YMbyDate:
    def __init__(self, bot_token:str = '', chats:str = '', message_type: str = '', subd:              str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', login: str = '', token: str  = '',  start: str = '', backfill_days: int = 0,
                 report: str = '', dimensions: str = '',  metrics: str = '', filters:str = None):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common =  Common(self.bot_token, self.chat_list, self.message_type)
        self.login = login
        self.token = token
        self.subd = subd
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.add_name = self.common.transliterate_key(add_name)
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.yesterday = self.today - timedelta(days=1)
        self.start = start
        self.dimensions = dimensions
        self.report = report
        self.metrics = metrics
        self.err429 = False
        self.filters = filters
        self.backfill_days = backfill_days

        self.source_dict = {
            'date': {
                'platform': 'ym',
                'report_name': 'date',
                'upload_table': 'date',
                'func_name': self.get_stat,
                'uniq_columns': 'date',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
            'nodate': {
                'platform': 'ym',
                'report_name': 'nodate',
                'upload_table': 'nodate',
                'func_name': self.get_data,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
        }

    def get_metrika_data(self,date1, date2,limit, offset):
        try:
            url = "https://api-metrika.yandex.ru/stat/v1/data"
            headers = {"Authorization": f"OAuth {self.token}"}
            params = {
                "ids": self.login,
                "metrics": self.metrics,
                "dimensions": self.dimensions,
                "date1": date1,
                "date2": date2,
                "limit": limit,
                'accuracy': 'full'
            }
            if offset != 0 and offset != '0':
                params["offset"] = offset
            if self.filters:
                params["filters"] = self.filters
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                response.raise_for_status()
            return response.json()
        except Exception as e:
            message = f'Платформа: YM. Имя: {self.add_name}. Даты: {str(date1)}-{str(date2)}. Функция: get_metrika_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def fetch_all_metrika_data(self,  date1, date2):
        try:
            all_data = []
            all_data_transformed = []
            dimension_list = self.dimensions.split(',')
            metrics_list = self.metrics.split(',')
            offset = 0
            limit = 10000
            while True:
                data = self.get_metrika_data( date1, date2, limit, offset)
                rows = data.get("data", [])
                all_data.extend(rows)
                if len(rows) < limit:
                    break
                offset += limit
                time.sleep(1)
            for row in all_data:
                transformed_element = {}
                dim_num = 0
                for dim in row['dimensions']:
                    transformed_element[dimension_list[dim_num].split(':')[-1].strip()] = dim['name']
                    dim_num += 1
                met_num = 0
                for met in row['metrics']:
                    transformed_element[metrics_list[met_num].split(':')[-1].strip()] = met
                    met_num += 1
                all_data_transformed.append(transformed_element)
            return all_data_transformed
        except Exception as e:
            message = f'Платформа: YM. Имя: {self.add_name}. Даты: {str(date1)}-{str(date2)}. Функция: fetch_all_metrika_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_data(self, date=''):
        try:
            final_result = self.fetch_all_metrika_data(self.start, self.yesterday.strftime('%Y-%m-%d'))
            message = f'Платформа: YM. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: YM. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message


    def get_stat(self, date):
        try:
            final_result = self.fetch_all_metrika_data(date, date)
            message = f'Платформа: YM. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_stat. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: YM. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_stat. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message


    def collecting_manager(self):
        if self.report == 'nodate':
            self.platform = 'ym_nodate'
        elif self.report == 'date':
            self.platform = 'ym_date'
        self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port, self.username, self.password, self.database,
                                     self.start, self.add_name, self.err429, self.backfill_days, self.platform)
        self.clickhouse.collecting_report(
            self.source_dict[self.report]['platform'],
            self.source_dict[self.report]['report_name'],
            self.source_dict[self.report]['upload_table'],
            self.source_dict[self.report]['func_name'],
            self.source_dict[self.report]['uniq_columns'],
            self.source_dict[self.report]['partitions'],
            self.source_dict[self.report]['merge_type'],
            self.source_dict[self.report]['refresh_type'],
            self.source_dict[self.report]['history'],
            self.source_dict[self.report]['frequency'],
            self.source_dict[self.report]['delay']
        )
        self.common.send_logs_clear_anyway(self.bot_token, self.chat_list)








