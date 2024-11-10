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
import math


class YDbyDate:
    def __init__(self, logging_path:str, subd: str, add_name: str, login: str, token: str , host: str, port: str,
                 username: str, password: str, database: str, start: str, backfill_days: int,
                 columns : str,  report: str, goals :str = None, attributions :str = None):
        self.logging_path = os.path.join(logging_path,f'yd_logs.log')
        self.common = Common(self.logging_path)
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

        logging.basicConfig(filename=self.logging_path, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
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
        tsv_data = response.text
        data = StringIO(tsv_data)
        df = pd.read_csv(data, sep='\t')
        list_of_dicts = df.to_dict(orient='records')
        return list_of_dicts


    # дата+токен -> список словарей с заказами (данные)
    def get_report(self, date1, date2):
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
        try:
            response = requests.post('https://api.direct.yandex.com/json/v5/reports', headers=headers, json=data)
            start_code = str(response.status_code)
            print(f'Результат первого запроса: {start_code}')
            logging.info(f'Результат первого запроса: {start_code}')
            if start_code == '200':
                return self.tsv_to_dict(response)
            if start_code == '201':
                for i in range(60):
                    time.sleep(10)
                    response = requests.post('https://api.direct.yandex.com/json/v5/reports', headers=headers,
                                             json=data)
                    code = str(response.status_code)
                    if code == '200':
                        return self.tsv_to_dict(response)
                        break
            else:
                print(f'Ошибка. Код ответа: {start_code}')
                logging.info(f'Ошибка. Код ответа: {start_code}')
                response.raise_for_status()
        except Exception as e:
            print(f'Ошибка: {e}')
            logging.info(f'Ошибка: {e}')
            raise

    def get_data(self, date):
        return self.get_report(self.start, self.yesterday.strftime('%Y-%m-%d'))

    def get_stat(self, date):
        return self.get_report(date, date)

    def get_campaigns(self):
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
        try:
            response = requests.post(campaigns_url, data=jsonData, headers=headers)
            camp_data = response.json()['result']['Campaigns']
            return camp_data
        except Exception as e:
            print(f"Ошибка получения кампаний: {e}")
            logging.info(f'Ошибка получения кампаний: {e}')


    def get_ads(self, campaign_id, offset):
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
        try:
            response = requests.post(ads_url, data=jsonBody, headers=headers)
            ads_data = response.json()
            return ads_data['result']['Ads']
        except Exception as e:
            print(f"Ошибка получения объявлений по кампании: {campaign_id}")
            logging.info(f'Ошибка получения объявлений по кампании: {campaign_id}')


    def collect_campaign_ads(self, date):
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
            return final_list
        except Exception as e:
            print(f"Ошибка сбора всех объявлений: {e}")
            logging.info(f'Ошибка сбора всех объявлений: {e}')


    def collecting_manager(self):
        if self.report == 'ads':
            self.platform = 'yd_ads'
        elif self.report == 'nodate':
            self.platform = 'yd_nodate'
        elif self.report == 'date':
            self.platform = 'yd_date'
        self.clickhouse = Clickhouse(self.logging_path, self.host, self.port, self.username, self.password, self.database,
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
        self.common.keep_last_20000_lines(self.logging_path)






