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


class BTRXbyDate:
    def __init__(self, bot_token:str, chats:str, message_type: str, subd: str,
                 host: str, port: str, username: str, password: str, database: str,
                 add_name: str, webhook_link: str ,  start: str, backfill_days: int, reports :str):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.webhook_link = webhook_link
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
        self.platform = 'btrx'
        self.err429 = False
        self.source_dict = {
            'leads': {
                'platform': 'btrx',
                'report_name': 'leads',
                'upload_table': 'leads',
                'func_name': self.get_leads_create,
                'uniq_columns': 'ID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
                        'contacts': {
                'platform': 'btrx',
                'report_name': 'contacts',
                'upload_table': 'contacts',
                'func_name': self.get_contacts_create,
                'uniq_columns': 'ID,DATE_CREATE',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
            'deals': {
                'platform': 'btrx',
                'report_name': 'deals',
                'upload_table': 'deals',
                'func_name': self.get_deals_create,
                'uniq_columns': 'ID,DATE_CREATE',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
            'leads_changes': {
                'platform': 'btrx',
                'report_name': 'leads_changes',
                'upload_table': 'leads',
                'func_name': self.get_leads_modify,
                'uniq_columns': 'ID,DATE_CREATE',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
            'contacts_changes': {
                'platform': 'btrx',
                'report_name': 'contacts_changes',
                'upload_table': 'contacts',
                'func_name': self.get_contacts_modify,
                'uniq_columns': 'ID,DATE_CREATE',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
            'deals_changes': {
                'platform': 'btrx',
                'report_name': 'deals_changes',
                'upload_table': 'deals',
                'func_name': self.get_deals_modify,
                'uniq_columns': 'ID,DATE_CREATE',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 10
            },
        }

    def get_bitrix_data(self, report, filter_column, date1, date2):
        try:
            all_leads = []
            offset = 0
            while True:
                url = f'{self.webhook_link}/{report}?limit=50&start={offset}&filter[>{filter_column}]={date1}T00:00:00&filter[<{filter_column}]={date2}T23:59:59'
                response = requests.get(url)
                code =       response.status_code
                if code != 200:
                    response.raise_for_status()
                else:
                    data = response.json()['result']
                    all_leads += data
                if len(data) < 50:
                    break
                offset += 50
            return all_leads
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Даты: {str(date1)}-{str(date2)}. Функция: get_bitrix_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_leads_create(self, date):
        try:
            final_result = self.get_bitrix_data('crm.lead.list.json', 'DATE_CREATE', date, date)
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_leads_create. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_leads_create. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_contacts_create(self, date):
        try:
            final_result = self.get_bitrix_data('crm.contact.list.json', 'DATE_CREATE', date, date)
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_contacts_create. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_contacts_create. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_deals_create(self, date):
        try:
            final_result = self.get_bitrix_data('crm.deal.list.json', 'DATE_CREATE', date, date)
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_deals_create. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_deals_create. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_leads_modify(self, date):
        try:
            final_result = self.get_bitrix_data('crm.lead.list.json', 'DATE_MODIFY', date, date)
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_leads_modify. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_leads_modify. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_contacts_modify(self, date):
        try:
            final_result = self.get_bitrix_data('crm.contact.list.json', 'DATE_MODIFY', date, date)
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_contacts_modify. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_contacts_modify. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_deals_modify(self, date):
        try:
            final_result = self.get_bitrix_data('crm.deal.list.json', 'DATE_MODIFY', date, date)
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_deals_modify. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: BTRX. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_deals_modify. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message



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









