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


class YDbyDate:
    def __init__(self, bot_token:str = '', chats:str = '', message_type: str = '', subd:              str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', login: str = '', token: str  = '',  start: str = '', backfill_days: int = 0,
                 columns : str = '',  report: str = '', goals :str = None, attributions :str = None):
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
        self.columns = columns
        self.report = report
        self.goals = goals
        self.err429 = False
        self.attributions = attributions
        self.backfill_days = backfill_days

        self.source_dict = {
            'date': {
                'platform': 'yd',
                'report_name': 'date',
                'upload_table': 'date',
                'func_name': self.get_stat,
                'uniq_columns': 'Date',
                'partitions': 'Date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            },
            'nodate': {
                'platform': 'yd',
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
            'ads': {
                'platform': 'yd',
                'report_name': 'ads',
                'upload_table': 'ads',
                'func_name': self.collect_campaign_ads,
                'uniq_columns': 'AdId',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',  # '2dayOfMonth,Friday'
                'delay': 20
            }
        }

    def tsv_to_dict(self, response):
        try:
            tsv_data = response.text
            data = StringIO(tsv_data)
            df = pd.read_csv(data, sep='\t')
            list_of_dicts = df.to_dict(orient='records')
            return list_of_dicts
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Функция: tsv_to_dict. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise


    # дата+токен -> список словарей с заказами (данные)
    def get_report(self, date1, date2):
        try:
            current_hour = self.now.hour
            report_name = self.common.shorten_text(str(date1)+str(date2) + str(self.today) + str(self.login) + str(self.columns)
                                                   + str(self.goals) + str(self.attributions) + str(current_hour))
            headers = {
                "Authorization": "Bearer " + self.token,
                "Client-Login": self.login,
                "Accept-Language": "ru", "processingMode": "auto", "returnMoneyInMicros": "false",
                "skipReportHeader": "true", "skipColumnHeader": "false", "skipReportSummary": "true"
            }
            dict = {
                "SelectionCriteria": {"DateFrom": date1, "DateTo": date2},
                "FieldNames": self.columns.replace(' ', '').split(','),
                "ReportName": report_name,
                "Page": {"Limit": 5000000},
                "ReportType": "CUSTOM_REPORT", "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV", "IncludeVAT": "YES", "IncludeDiscount": "NO"
            }
            if self.goals != None and self.goals != '':
                goals_list = list(map(int, self.goals.replace(' ', '').split(',')))
                goal_dict = {"Goals": goals_list}
                dict = dict | goal_dict
            if self.attributions != None and self.attributions != '':
                att_dict = {"AttributionModels": self.attributions.replace(' ', '').split(',')}
                dict = dict | att_dict
            data = {"params": dict}
            response = requests.post('https://api.direct.yandex.com/json/v5/reports', headers=headers, json=data)
            start_code = response.status_code
            if start_code == 200:
                final_result= self.tsv_to_dict(response)
            elif start_code == 201:
                for i in range(60):
                    time.sleep(10)
                    response = requests.post('https://api.direct.yandex.com/json/v5/reports', headers=headers, json=data)
                    code = response.status_code
                    if code == 200:
                        final_result = self.tsv_to_dict(response)
                        break
            else:
                response.raise_for_status()
            return final_result
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Даты: {str(date1)}-{str(date2)}. Функция: get_report. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise

    def get_data(self, date=''):
        try:
            final_result = self.get_report(self.start, self.yesterday.strftime('%Y-%m-%d'))
            message = f'Платформа: YD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message


    def get_stat(self, date):
        try:
            final_result = self.get_report(date, date)
            message = f'Платформа: YD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_stat. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Даты: {str(date)}. Функция: get_stat. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return  message


    def get_campaigns(self):
        try:
            campaigns_url = 'https://api.direct.yandex.com/json/v5/campaigns'
            headers = {"Authorization": "Bearer " + self.token,
                "Client-Login": self.login,
                "Accept-Language": "ru",
                "Content-Type": "application/json"}
            data = {"method": "get",
                "params": {
                    "SelectionCriteria": {},
                    "FieldNames": ["Id", "Name"]}}
            jsonData = json.dumps(data, ensure_ascii=False).encode('utf8')
            response = requests.post(campaigns_url, data=jsonData, headers=headers)
            camp_data = response.json()['result']['Campaigns']
            return camp_data
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Функция: get_campaigns. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise



    def get_ads(self, campaign_id, offset):
        try:
            ads_url = 'https://api.direct.yandex.com/json/v5/ads'
            headers = {
                "Authorization": "Bearer " + self.token,
                "Client-Login": self.login,
                "Accept-Language": "ru",
                "Content-Type": "application/json",
            }
            body = {"method": "get",
                    "params": {"SelectionCriteria": {"CampaignIds": [int(campaign_id)]},
                    "FieldNames": [ "CampaignId", "Id", "State", "Status"],
                    "TextAdFieldNames":["Title", "Title2" ,"Text", "Href"],
                    "Page": { "Limit": 10000, "Offset": offset }
                    }}
            jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
            response = requests.post(ads_url, data=jsonBody, headers=headers)
            ads_data = response.json()
            return ads_data['result']['Ads']
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Функция: get_ads. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise



    def collect_campaign_ads(self, date=''):
        try:
            final_list = []
            datestr = self.today.strftime('%Y-%m-%d')
            campaigns = self.get_campaigns()
            for camp in campaigns:
                camp_id = camp['Id']
                offset = 0
                try:
                    for k in range(10):
                        ads = self.get_ads(camp_id, offset)
                        for row in ads:
                            final_list.append({'Date': datestr,'CampaignName': camp['Name'], 'CampaignId': camp['Id'], 'AdId': row['Id'], 'Title': row['TextAd']['Title'],'Title2': row['TextAd']['Title2'], 'Text': row['TextAd']['Text'], 'Href': row['TextAd']['Href'] })
                        if len(ads)<10000:
                            break
                        offset += 10000
                except:
                    pass
            message = f'Платформа: YD. Имя: {self.add_name}. Дата: {str(date)}. Функция: collect_campaign_ads. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_list
        except Exception as e:
            message = f'Платформа: YD. Имя: {self.add_name}. Даты: {str(date)}. Функция: collect_campaign_ads. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message



    def collecting_manager(self):
        if self.report == 'ads':
            self.platform = 'yd_ads'
        elif self.report == 'nodate':
            self.platform = 'yd_nodate'
        elif self.report == 'date':
            self.platform = 'yd_date'
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








