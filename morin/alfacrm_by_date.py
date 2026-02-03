from .common import Common
from .clickhouse import Clickhouse
import requests
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
import os
import math
from dateutil import parser
import time
import hashlib
from io import StringIO
import json
from dateutil.relativedelta import relativedelta


class ALFAbyDate:
    def __init__(self,  bot_token:str  = '', chats:str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                                  add_name: str = '', main_url:str = '', token: str  = '',  xappkey:str = '', email: str  = '',
                 start: str = '', backfill_days: int = 0, reports :str = '', branches: str = ''):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.main_url = main_url
        self.token = token
        self.branches_list = branches.replace(' ','').strip().split(',')
        self.xappkey = xappkey
        self.email = email
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
        self.platform = 'alfa'
        self.err429 = False
        self.source_dict = {
            'branch': {
                'platform': 'alfa',
                'report_name': 'branch',
                'upload_table': 'branch',
                'func_name': self.get_branch,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'location': {
                'platform': 'alfa',
                'report_name': 'location',
                'upload_table': 'location',
                'func_name': self.get_location,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'room': {
                'platform': 'alfa',
                'report_name': 'room',
                'upload_table': 'room',
                'func_name': self.get_room,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'sms': {
                'platform': 'alfa',
                'report_name': 'sms',
                'upload_table': 'sms',
                'func_name': self.get_sms,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'mail': {
                'platform': 'alfa',
                'report_name': 'mail',
                'upload_table': 'mail',
                'func_name': self.get_mail,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'call': {
                'platform': 'alfa',
                'report_name': 'call',
                'upload_table': 'call',
                'func_name': self.get_call,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_sms': {
                'platform': 'alfa',
                'report_name': 'sms',
                'upload_table': 'sms',
                'func_name': self.get_all_sms,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_mail': {
                'platform': 'alfa',
                'report_name': 'mail',
                'upload_table': 'mail',
                'func_name': self.get_all_mail,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_call': {
                'platform': 'alfa',
                'report_name': 'call',
                'upload_table': 'call',
                'func_name': self.get_all_call,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'communication': {
                'platform': 'alfa',
                'report_name': 'communication',
                'upload_table': 'communication',
                'func_name': self.get_communication,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'user': {
                'platform': 'alfa',
                'report_name': 'user',
                'upload_table': 'user',
                'func_name': self.get_user,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'lead_reject': {
                'platform': 'alfa',
                'report_name': 'lead_reject',
                'upload_table': 'lead_reject',
                'func_name': self.get_lead_reject,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'customer_reject': {
                'platform': 'alfa',
                'report_name': 'customer_reject',
                'upload_table': 'customer_reject',
                'func_name': self.get_customer_reject,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'task_null': {
                'platform': 'alfa',
                'report_name': 'task_null',
                'upload_table': 'task',
                'func_name': self.get_task_null,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'tariff': {
                'platform': 'alfa',
                'report_name': 'tariff',
                'upload_table': 'tariff',
                'func_name': self.get_tariff,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'teacher': {
                'platform': 'alfa',
                'report_name': 'teacher',
                'upload_table': 'teacher',
                'func_name': self.get_teacher,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'customer_tariff': {
                'platform': 'alfa',
                'report_name': 'customer_tariff',
                'upload_table': 'customer_tariff',
                'func_name': self.get_customer_tariff,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'bonus': {
                'platform': 'alfa',
                'report_name': 'bonus',
                'upload_table': 'bonus',
                'func_name': self.get_bonus,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'cgi': {
                'platform': 'alfa',
                'report_name': 'cgi',
                'upload_table': 'cgi',
                'func_name': self.get_cgi,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'teacher_hour': {
                'platform': 'alfa',
                'report_name': 'teacher_hour',
                'upload_table': 'teacher_hour',
                'func_name': self.get_teacher_hour,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'teacher_rate': {
                'platform': 'alfa',
                'report_name': 'teacher_rate',
                'upload_table': 'teacher_rate',
                'func_name': self.get_teacher_rate,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'subject': {
                'platform': 'alfa',
                'report_name': 'subject',
                'upload_table': 'subject',
                'func_name': self.get_subject,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'study_status': {
                'platform': 'alfa',
                'report_name': 'study_status',
                'upload_table': 'study_status',
                'func_name': self.get_study_status,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'discount': {
                'platform': 'alfa',
                'report_name': 'discount',
                'upload_table': 'discount',
                'func_name': self.get_discount,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'lead_status': {
                'platform': 'alfa',
                'report_name': 'lead_status',
                'upload_table': 'lead_status',
                'func_name': self.get_lead_status,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'lead_source': {
                'platform': 'alfa',
                'report_name': 'lead_source',
                'upload_table': 'lead_source',
                'func_name': self.get_lead_source,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'customer': {
                'platform': 'alfa',
                'report_name': 'customer',
                'upload_table': 'customer',
                'func_name': self.get_customer,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'regular_lesson': {
                'platform': 'alfa',
                'report_name': 'regular_lesson',
                'upload_table': 'regular_lesson',
                'func_name': self.get_regular_lesson,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'group': {
                'platform': 'alfa',
                'report_name': 'group',
                'upload_table': 'group',
                'func_name': self.get_group,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'pay': {
                'platform': 'alfa',
                'report_name': 'pay',
                'upload_table': 'pay',
                'func_name': self.get_pay,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'log': {
                'platform': 'alfa',
                'report_name': 'log',
                'upload_table': 'log',
                'func_name': self.get_log,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'fresh_log': {
                'platform': 'alfa',
                'report_name': 'fresh_log',
                'upload_table': 'log',
                'func_name': self.get_fresh_log,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'task': {
                'platform': 'alfa',
                'report_name': 'task',
                'upload_table': 'task',
                'func_name': self.get_task,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'lesson': {
                'platform': 'alfa',
                'report_name': 'lesson',
                'upload_table': 'lesson',
                'func_name': self.get_lesson,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'future_lesson': {
                'platform': 'alfa',
                'report_name': 'future_lesson',
                'upload_table': 'lesson',
                'func_name': self.get_future_lesson,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'update_customer': {
                'platform': 'alfa',
                'report_name': 'update_customer',
                'upload_table': 'customer',
                'func_name': self.update_customer,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'update_group': {
                'platform': 'alfa',
                'report_name': 'update_group',
                'upload_table': 'group',
                'func_name': self.update_group,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'update_pay': {
                'platform': 'alfa',
                'report_name': 'update_pay',
                'upload_table': 'pay',
                'func_name': self.update_pay,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'update_lesson': {
                'platform': 'alfa',
                'report_name': 'update_lesson',
                'upload_table': 'lesson',
                'func_name': self.update_lesson,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_customer': {
                'platform': 'alfa',
                'report_name': 'all_customer',
                'upload_table': 'customer',
                'func_name': self.all_customer,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_regular_lesson': {
                'platform': 'alfa',
                'report_name': 'all_regular_lesson',
                'upload_table': 'regular_lesson',
                'func_name': self.all_regular_lesson,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_group': {
                'platform': 'alfa',
                'report_name': 'all_group',
                'upload_table': 'group',
                'func_name': self.all_group,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_pay': {
                'platform': 'alfa',
                'report_name': 'all_pay',
                'upload_table': 'pay',
                'func_name': self.all_pay,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
            'all_lesson': {
                'platform': 'alfa',
                'report_name': 'all_lesson',
                'upload_table': 'lesson',
                'func_name': self.all_lesson,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 1
            },
        }

    def auth(self):
        try:
            url = f"{self.main_url.rstrip('/')}/v2api/auth/login"
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-APP-KEY": self.xappkey
            }
            data = {
                "email": self.email,
                "api_key": self.token
            }
            response = requests.post(url, headers=headers, data=json.dumps(data))
            code = response.status_code
            if code != 200:
                response.raise_for_status()
            else:
                result = response.json()
                self.access_token = result.get("token")
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Функция: auth. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise


    def get_basic(self,url,filter_1=None, filter_2=None,filter_3=None, filter_4=None):
        try:
            all_data = []
            detector = True
            id0 = 0
            page = 0
            url = f"{self.main_url.rstrip('/')}/{url}"
            headers = {
                "X-ALFACRM-TOKEN": self.access_token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            while True:
                payload = {"page": page}
                if filter_1:
                    payload = payload | filter_1
                if filter_2 :
                    payload = payload | filter_2
                if filter_3:
                    payload = payload | filter_3
                if filter_4:
                    payload = payload | filter_4
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 401:
                    self.auth()
                    time.sleep(1)
                    response = requests.post(url, headers=headers, data=json.dumps(payload))
                    code = response.status_code
                if code != 200:
                    response.raise_for_status()
                else:
                    result = response.json()
                    data = result['items']
                    count = math.ceil(int(result['total'])/50)
                    print(f'URL: {url}. Всего страниц: {str(count)}. Страница: {str(page)}.' )
                    if len(data) > 0:
                        id1 = int(data[0]['id'])
                        if id0 == id1:
                            break
                        all_data += data
                if len(data) < 50:
                    break
                page +=1
                time.sleep(0.7)
                id0 = id1
            return self.common.replace_keys_in_data(all_data)
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Функция: get_basic. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_basic_bonus(self,url):
        try:
            all_data = []
            url = f"{self.main_url.rstrip('/')}/{url}"
            headers = {
                "X-ALFACRM-TOKEN": self.access_token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            response = requests.post(url, headers=headers)
            code = response.status_code
            if code == 401:
                self.auth()
                time.sleep(1)
                response = requests.post(url, headers=headers)
                code = response.status_code
            if code != 200:
                response.raise_for_status()
            else:
                result = response.json()
                result['id'] = url.split('customer_id=')[1]
                all_data.append(result)
            return all_data
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Функция: get_basic_bonus. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise


    def get_branch(self, date):
        try:
            final_result = self.get_basic('v2api/branch/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_branch. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_branch. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_subject(self, date):
        try:
            final_result = self.get_basic('v2api/subject/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_subject. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_subject. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_study_status(self, date):
        try:
            final_result = self.get_basic('v2api/study-status/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_study_status. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_study_status. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_room(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/room/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_room. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_room. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_sms(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/sms-message', {'date_to': self.common.shift_date(date,-1)},{'date_from': date})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sms. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_sms. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_mail(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/mail-message', {'date_to': self.common.shift_date(date,-1)},{'date_from': date})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_mail. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_mail. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_call(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/phone-call', {'date_to': self.common.shift_date(date,-1)},{'date_from': date})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_call. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_call. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_all_sms(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/sms-message')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_all_sms. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_all_sms. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_all_mail(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/mail-message')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_all_mail. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_all_mail. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_all_call(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/phone-call')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_all_call. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_all_call. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_communication(self, date):
        try:
            final_result = []
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
            customer_list = self.clickhouse.get_table_data(f'{self.platform}_customer_{self.add_name}', ' id, branch_ids ')
            for customer in customer_list:
                for branch in json.loads(customer['branch_ids']):
                    final_result += self.get_basic(f"v2api/{str(branch).strip()}/communication/index?class=Customer&related_id={str(int(customer['id']))}")
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_communication. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_communication. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_customer_reject(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                    final_result += self.get_basic(f'v2api/{str(branch).strip()}/customer-reject/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_customer_reject. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_customer_reject. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_customer_tariff(self, date):
        try:
            final_result = []
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
            customer_list = self.clickhouse.get_table_data(f'{self.platform}_customer_{self.add_name}', ' id, branch_ids ')
            for customer in customer_list:
                for branch in json.loads(customer['branch_ids']):
                    final_result += self.get_basic(f"v2api/{str(branch).strip()}/customer-tariff/index?customer_id={str(int(customer['id']))}")
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_customer_tariff. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_customer_tariff. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_bonus(self, date):
        try:
            final_result = []
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
            customer_list = self.clickhouse.get_table_data(f'{self.platform}_customer_{self.add_name}', ' id, branch_ids ')
            for customer in customer_list:
                for branch in json.loads(customer['branch_ids']):
                    final_result += self.get_basic_bonus(f"v2api/{str(branch).strip()}/bonus/balance-bonus?customer_id={str(int(customer['id']))}")
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_bonus. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_bonus. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_cgi(self, date):
        try:
            final_result = []
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
            group_list = self.clickhouse.get_table_data(f'{self.platform}_group_{self.add_name}', ' id, branch_ids ')
            for group in group_list:
                for branch in json.loads(group['branch_ids']):
                    final_result += self.get_basic(f"v2api/{str(branch).strip()}/cgi/index?group_id={str(int(group['id']))}")
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_cgi. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_cgi. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_lead_reject(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lead-reject/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lead_reject. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lead_reject. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_user(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                    final_result += self.get_basic(f'v2api/{str(branch).strip()}/user/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_user. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_user. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_lead_status(self, date):
        try:
            final_result = self.get_basic('v2api/lead-status/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lead_status. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lead_status. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message
    def get_discount(self, date):
        try:
            final_result = self.get_basic('v2api/discount/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_discount. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_discount. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message
    def get_lead_source(self, date):
        try:
            final_result = self.get_basic('v2api/lead-source/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lead_source. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lead_source. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_location(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/location/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_location. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_location. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_group(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/group/index',{'created_at_to': self.common.flip_date(self.common.shift_date(date,-1))},{'created_at_from': self.common.flip_date(date)},{'removed': 1})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_group. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_group. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_customer(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/customer/index', {'date_to': self.common.shift_date(date,-1)},{'date_from': date}, {'removed': 1},{'is_study': 2})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_customer. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_customer. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message
    def get_regular_lesson(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/regular-lesson/index', {'date_to': self.common.shift_date(date,-1)},{'date_from': date})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_regular_lesson. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_regular_lesson. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message
    def get_tariff(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/tariff/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_tariff. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_tariff. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message
    def get_task(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/task/index', {'due_date_from': date},{'due_date_to': self.common.shift_date(date,-1)})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_task. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_task. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_task_null(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/task/index', {'due_date_is_null': True})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_task_null. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_task_null. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_lesson(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index', {'date_to': self.common.shift_date(date,-1)},
                                               {'date_from': date}, {'status': 2})
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index', {'date_to': self.common.shift_date(date,-1)},
                                               {'date_from': date}, {'status': 3})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lesson. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_lesson. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_future_lesson(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index',
                                               {'date_from': date}, {'status': 1})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_future_lesson. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_future_lesson. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_pay(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/pay/index', {'date_to': self.common.shift_date(date,-1)},{'date_from': date})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_pay. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_pay. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_log(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/log/index', {'date_to': self.common.flip_date(date)},{'date_from': self.common.flip_date(date)})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_log. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_log. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_fresh_log(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/log/index', {'date_from': self.common.flip_date(date)})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_fresh_log. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_fresh_log. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def update_customer(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/customer/index', {'updated_at_to': self.common.flip_date(self.common.shift_date(date,-1))},{'updated_at_from': self.common.flip_date(date)},{'removed': 1},{'is_study': 2})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_customer. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_customer. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def update_group(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/group/index', {'updated_at_to': self.common.flip_date(self.common.shift_date(date,-1))},{'updated_at_from': self.common.flip_date(date)},{'removed': 1})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_group. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_group. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def update_lesson(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index',
                                               {'updated_at_to': self.common.flip_date(self.common.shift_date(date,-1))},
                                               {'updated_at_from': self.common.flip_date(date)}, {'status': 2})
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index',
                                               {'updated_at_to': self.common.flip_date(self.common.shift_date(date,-1))},
                                               {'updated_at_from': self.common.flip_date(date)}, {'status': 3})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_lesson. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_lesson. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def update_pay(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/pay/index', {'updated_at_to': self.common.flip_date(self.common.shift_date(date,-1))},{'updated_at_from': self.common.flip_date(date)})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_pay. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: update_pay. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def all_customer(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/customer/index',{'removed': 1},{'is_study': 2})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_customer. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_customer. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message
    def all_regular_lesson(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/regular-lesson/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_regular_lesson. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_regular_lesson. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def all_group(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/group/index',{'removed': 1})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_group. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_group. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def all_lesson(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index', {'status': 2})
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/lesson/index', {'status': 3})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_lesson. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_lesson. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def all_pay(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/pay/index')
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_pay. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: all_pay. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_teacher(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/teacher/index', {'removed':1})
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_teacher. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_teacher. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_teacher_rate(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/teacher/teacher-rate', )
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_teacher_rate. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_teacher_rate. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_teacher_hour(self, date):
        try:
            final_result = []
            for branch in self.branches_list:
                final_result += self.get_basic(f'v2api/{str(branch).strip()}/teacher/working-hour', )
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_teacher_hour. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: ALFA. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_teacher_hour. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def collecting_manager(self):
        self.auth()
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
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


