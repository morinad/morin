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


class OZONbyDate:
    def __init__(self, logging_path:str, subd: str, add_name: str, clientid:str, token: str , host: str, port: str, username: str, password: str, database: str, start: str, backfill_days: int, reports :str):
        self.clientid = clientid
        self.token = token
        self.subd = subd
        self.add_name = add_name.replace(' ','').replace('-','_')
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.common = Common(logging_path)
        self.err429 = False
        self.clickhouse = Clickhouse(logging_path, host, port, username, password, database)
        logging.basicConfig(filename=logging_path,level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    def get_transaction_page_count(self, date):
        try:
            url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
            headers = {"Client-Id": self.clientid,
                       "Api-Key": self.token,
                       "Content-Type": "application/json"}
            payload = {
                "filter": {
                    "date": {"from": f"{date}T00:00:00.000Z",
                             "to": f"{date}T23:59:59.999Z"},
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all"},
                "page": 1,
                "page_size": 1000
            }
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            code = response.status_code
            if code == 200:
                page_count = response.json()['result']['page_count']
                return page_count
            elif code == 429:
                self.err429 = True
            else:
                response.raise_for_status()
        except Exception as e:
            print(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            logging.info(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            return e


    def get_transactions(self, date):
        try:
            url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
            headers = {"Client-Id": self.clientid,
                       "Api-Key": self.token,
                       "Content-Type": "application/json"}
            page_count = int(self.get_transaction_page_count(date))
            operations = []
            for page in range(1, page_count + 1):
                print(page)
                payload = {
                    "filter": {
                        "date": {"from": f"{date}T00:00:00.000Z",
                                 "to": f"{date}T23:59:59.999Z"},
                        "operation_type": [],
                        "posting_number": "",
                        "transaction_type": "all"},
                    "page": page,
                    "page_size": 1000
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                code = response.status_code
                if code == 200:
                    operations += response.json()['result']['operations']
                    return operations
                elif code == 429:
                    self.err429 = True
                else:
                    response.raise_for_status()
                time.sleep(2)
        except Exception as e:
            print(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            logging.info(f'Ошибка: {e}. Дата: {date}. Запрос - транзакции.')
            return e

    # тип отчёта, дата -> данные в CH
    def upload_data(self, report, date):
        if self.err429 == False:
            try:
                reports = {'transactions':
                {'table_name': f'ozon_transactions{self.add_name}', 'uniq_columns': 'operation_date, operation_id','partitions': ''},

                    }
                table_name = reports[report]['table_name']
                uniq_columns = reports[report]['uniq_columns']
                partitions = reports[report]['partitions']
                if report == 'transactions': data = self.get_transactions(date)

                self.clickhouse.create_alter_ch(data, table_name, uniq_columns, partitions,'ReplacingMergeTree(timeStamp)')
                df = self.common.check_and_convert_types(data, uniq_columns, partitions)
                self.clickhouse.ch_insert(df, table_name)
                print(f'Данные добавлены. Репорт: {report}, Дата: {date}')
                logging.info(f'Данные добавлены. Репорт: {report}, Дата: {date}')
            except Exception as e:
                print(f'Ошибка вставки: {e}')
                logging.info(f'Ошибка вставки: {e}')
                raise
        else:
            raise ValueError("Обнаружена ошибка 429")

    def upload_report(self, report, date, collection_data):
        try:
            self.upload_data(report, date)
            self.clickhouse.ch_insert(collection_data, f'ozon_main_collection{self.add_name}')
            print(f'Успешно загружено. Репорт: {report}, Дата: {date}')
            logging.info(f'Успешно загружено. Репорт: {report}, Дата: {date}')
            time.sleep(60)
        except Exception as e:
            print(f'Ошибка: {e}! Репорт: {report}, Дата: {date}, ')
            logging.info(f'Ошибка: {e}! Репорт: {report}, Дата: {date}, ')
            time.sleep(60)

    # сбор orders, sales и realized
    def collecting_report(self, report):
        logging.info(f"Начинаем сбор {report} для клиента: {self.add_name}")
        print(f"Начинаем сбор {report} для клиента: {self.add_name}")
        n_days_ago = self.today - timedelta(days=self.backfill_days)
        create_table_query_collect = f"""
            CREATE TABLE IF NOT EXISTS ozon_main_collection{self.add_name} (
            date Date, report String, collect Bool ) ENGINE = ReplacingMergeTree(collect)
            ORDER BY (report, date)"""
        optimize_collection = f"OPTIMIZE TABLE ozon_main_collection{self.add_name} FINAL"
        self.clickhouse.ch_execute(create_table_query_collect)
        self.clickhouse.ch_execute(optimize_collection)
        time.sleep(10)
        date_list = self.clickhouse.get_missing_dates(f'ozon_main_collection{self.add_name}', report, self.start)
        for date in date_list:
            if self.err429 == False:
                print(f'Начинаем сбор. Репорт: {report}, Дата: {date}')
                logging.info(f'Начинаем сбор. Репорт: {report}, Дата: {date}')
                if datetime.strptime(date, '%Y-%m-%d').date() >= n_days_ago:
                    collect = False
                else:
                    collect = True
                collection_data = pd.DataFrame(
                    {'date': pd.to_datetime([date], format='%Y-%m-%d'), 'report': [report], 'collect': [collect]})
                self.upload_report(report, date, collection_data)


    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            if report in ['transactions', ]:
                self.collecting_report(report)
            if report in ['reklama']:
                pass


