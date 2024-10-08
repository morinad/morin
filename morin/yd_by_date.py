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
                 columns : str,  uniq_columns : str, goals :str = None, attributions :str = None):
        self.login = login
        self.token = token
        self.subd = subd
        self.add_name = add_name.replace(' ','').replace('-','_')
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.yesterday = self.today - timedelta(days=1)
        self.start = start
        self.columns = columns
        self.uniq_columns = uniq_columns
        self.goals = goals
        self.attributions = attributions
        self.backfill_days = backfill_days
        self.common = Common(logging_path)
        self.clickhouse = Clickhouse(logging_path, host, port, username, password, database)
        logging.basicConfig(filename=logging_path,level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
                for i in range(6):
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


    def collect_campaign_ads(self):
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


    def upload_data(self, report_type, date):
        try:
            table_name = f'yd_{report_type}{self.add_name}'
            if report_type == 'stat':
                partitions = 'Date'
                data = self.get_report(date, date)
                delete_partition = f"ALTER TABLE yd_{report_type}{self.add_name} DROP PARTITION '{date}';"
                self.clickhouse.create_alter_ch(data, table_name, self.uniq_columns, partitions, 'MergeTree')
            elif report_type == 'ads':
                partitions = ''
                data = self.collect_campaign_ads()
                self.clickhouse.create_alter_ch(data, table_name, self.uniq_columns, partitions,'ReplacingMergeTree(timeStamp)')
                delete_partition = f"OPTIMIZE TABLE yd_{report_type}{self.add_name};"
            else:
                partitions = ''
                data = self.get_report(self.start, self.yesterday.strftime('%Y-%m-%d'))
                delete_partition = f"TRUNCATE TABLE yd_{report_type}{self.add_name};"
                self.clickhouse.create_alter_ch(data, table_name, self.uniq_columns, partitions, 'MergeTree')
            df = self.common.check_and_convert_types(data, self.uniq_columns, partitions)
            self.clickhouse.ch_execute(delete_partition)
            self.clickhouse.ch_insert(df, table_name)
            print(f'Данные добавлены. Репорт: {self.add_name}, Дата: {date}')
            logging.info(f'Данные добавлены. Репорт: {self.add_name}, Дата: {date}')
        except Exception as e:
            print(f'Ошибка вставки: {e}')
            logging.info(f'Ошибка вставки: {e}')
            raise



    def upload_report(self, report_type, date, collection_data):
        try:
            self.upload_data(report_type, date)
            self.clickhouse.ch_insert(collection_data, f'yd_{report_type}_collection{self.add_name}')
            print(f'Успешно загружено. Репорт: {self.add_name}, Дата: {date}')
            logging.info(f'Успешно загружено. Репорт: {self.add_name}, Дата: {date}')
            time.sleep(10)
        except Exception as e:
            print(f'Ошибка: {e}! Репорт: {self.add_name}, Дата: {date}, ')
            logging.info(f'Ошибка: {e}! Репорт: {self.add_name}, Дата: {date}, ')
            time.sleep(60)


    def collecting_report(self, report_type, what):
        today_str = self.today.strftime('%Y-%m-%d')
        n_days_ago = self.today - timedelta(days=self.backfill_days)
        logging.info(f"Начинаем сбор {what} для: {self.add_name}")
        print(f"Начинаем сбор {what} для: {self.add_name}")
        create_table_query_collect = f"""
            CREATE TABLE IF NOT EXISTS yd_{report_type}_collection{self.add_name} (
            date Date, report String, collect Bool ) ENGINE = ReplacingMergeTree(collect)
            ORDER BY (date)"""
        optimize_collection = f"OPTIMIZE TABLE yd_{report_type}_collection{self.add_name} FINAL"
        self.clickhouse.ch_execute(create_table_query_collect)
        self.clickhouse.ch_execute(optimize_collection)
        time.sleep(10)
        if report_type == "stat":
            date_list = self.clickhouse.get_missing_dates(f'yd_{report_type}_collection{self.add_name}', self.add_name, self.start)
            for date in date_list:
                print(f'Начинаем сбор {what}, Репорт: {self.add_name}, Дата: {date}')
                logging.info(f'Начинаем сбор {what}, Репорт: {self.add_name}, Дата: {date}')
                if datetime.strptime(date, '%Y-%m-%d').date() >= n_days_ago:
                    collect = False
                else:
                    collect = True
                collection_data = pd.DataFrame({'date': pd.to_datetime([date], format='%Y-%m-%d'), 'report': [self.add_name],'collect': [collect]})
                self.upload_report(report_type, date, collection_data)
        else:
            print(f'Начинаем сбор {what}. Репорт: {self.add_name}, Дата: {today_str}')
            logging.info(f'Начинаем сбор {what}. Репорт: {self.add_name}, Дата: {today_str}')
            collection_data = pd.DataFrame(
                {'date': pd.to_datetime([today_str], format='%Y-%m-%d'), 'report': [self.add_name], 'collect': [True]})
            self.upload_report(report_type, today_str, collection_data)


    def collecting_manager(self):
        if self.columns == 'ads':
            self.collecting_report('ads', "объявлений")
        elif self.uniq_columns !='Date':
            self.collecting_report('data', 'данных')
        else:
            self.collecting_report('stat', 'статистики по дням')




