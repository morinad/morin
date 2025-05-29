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
from dateutil.relativedelta import relativedelta


class OZONANbyDate:
    def __init__(self,  bot_token:str = '', chats:str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                                  add_name: str = '', clientid:str = '', token: str  = '',  start: str = '', backfill_days: int = 0,
                 dimensions : str = 'day', metrics : str = 'hits_view',reports :str = ''):
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
        self.platform = 'ozonan'
        self.err429 = False
        self.dimensions = dimensions
        self.metrics = metrics
        self.dim = [d.strip() for d in self.dimensions.split(',')]
        self.met = [m.strip() for m in self.metrics.split(',')]
        self.source_dict = {
            'analytics': {
                'platform': 'ozonan',
                'report_name': 'analytics',
                'upload_table': 'analytics',
                'func_name': self.get_analytics,
                'uniq_columns': 'timeStamp',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily', # '2dayOfMonth,Friday'
                'delay': 60
            },
        }


    def get_analytics(self,date ):
        try:
            limit = 1000
            offset = 0
            result_all = []
            for k in range(25):
                url = "https://api-seller.ozon.ru/v1/analytics/data"
                headers = {
                    "Content-Type": "application/json",
                    "Api-Key": self.token,
                    "Client-Id": self.clientid
                }
                payload = {
                    "date_from": date,
                    "date_to": date,
                    "limit": limit,
                    "offset": offset,
                    "metrics": self.met,
                    "dimension": self.dim,
                    "sort": [{"key": self.met[0], "order": "DESC"}]
                }
                response = requests.post(url, headers=headers, json=payload)
                code = response.status_code
                print(code)
                if code == 429:
                    self.err429 = True
                if code != 200:
                    response.raise_for_status()
                res_json = response.json()
                data = res_json.get('result', {}).get('data', [])
                if len(data) == 0:
                    break
                elif len(data)<limit:
                    result_all.extend(data)
                    break
                else:
                    result_all.extend(data)
                offset+=limit
                time.sleep(60)
            normalized = []
            for row in result_all:
                flat = {}
                for key, dim in zip(self.dim, row['dimensions']):
                    flat[f'{key}_id'] = dim['id']
                    flat[f'{key}_name'] = dim['name']
                for key, val in zip(self.met, row['metrics']):
                    flat[key] = val
                normalized.append(flat)
            list_with_date = []
            for dict in normalized:
                dict['date'] = date
                list_with_date.append(dict)
            message = f'Платформа: OZONAN. Имя: {self.add_name}. Дата: {date}. Функция: get_analytics. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return list_with_date

        except Exception as e:
            message = f'Платформа: OZONAN. Имя: {self.add_name}. Дата: {date}. Функция: get_analytics. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return {"error": str(e)}


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




