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


class WBbyDate:
    def __init__(self, logging_path:str, subd: str, add_name: str, token: str , host: str, port: str, username: str, password: str, database: str, start: str, backfill_days: int, reports :str):
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

    # дата+токен -> список словарей с заказами (данные)
    def get_orders(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            headers = {
                "Authorization": self.token,
            }
            params = {
                "dateFrom": date_rfc3339,
                "flag": 1,  # Для получения всех заказов на указанную дату
            }
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            print(f'Код: {code}, запрос - orders')
            logging.info(f'Код: {code}, запрос - orders')
        except Exception as e:
            print(f'Ошибка: {e}, запрос - orders')
            logging.info(f'Ошибка: {e}, запрос - orders')
            return e

    def get_orders_changes(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            headers = {"Authorization": self.token}
            params = {"dateFrom": date_rfc3339}
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            print(f'Код: {code}, запрос - orders_changes')
            logging.info(f'Код: {code}, запрос - orders_changes')
        except Exception as e:
            print(f'Ошибка: {e}, запрос - orders_changes')
            logging.info(f'Ошибка: {e}, запрос - orders_changes')
            return e

    # дата+токен -> список словарей с заказами (данные)
    def get_sales(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            headers = {
                'Authorization': f'Bearer {self.token}'
            }
            params = {
                'dateFrom': date,
                "flag": 1,
            }
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            print(f'Код: {code}, запрос - sales')
            logging.info(f'Код: {code}, запрос - sales')
        except Exception as e:
            print(f'Ошибка: {e}, запрос - sales')
            logging.info(f'Ошибка: {e}, запрос - sales')
            return e

    def get_sales_changes(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            headers = {'Authorization': f'Bearer {self.token}'}
            params = {'dateFrom': date}
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            print(f'Код: {code}, запрос - sales_changes')
            logging.info(f'Код: {code}, запрос - sales_changes')
        except Exception as e:
            print(f'Ошибка: {e}, запрос - sales_changes')
            logging.info(f'Ошибка: {e}, запрос - sales_changes')
            return e

    # дата+токен -> список словарей с заказами (данные)
    def get_realized(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'
            headers = {'Authorization': f'Bearer {self.token}'}
            params = {'dateFrom': self.common.shift_date(date,7), 'dateTo': date}
            response = requests.get(url, headers=headers, params=params)
            code = str(response.status_code)
            if code == '200':
                return response.json()
            elif code == '429':
                self.err429 = True
            else:
                response.raise_for_status()
            print(f'Код: {code}, запрос - realized')
            logging.info(f'Код: {code}, запрос - realized')
        except Exception as e:
            print(f'Ошибка: {e}, запрос - realized')
            logging.info(f'Ошибка: {e}, запрос - realized')
            return e

    # тип отчёта, дата -> данные в CH
    def upload_data(self, report, date):
        if self.err429 == False:
            try:
                reports = {'orders': {'table_name':f'wb_orders{self.add_name}','uniq_columns':'date, srid', 'partitions':'warehouseName' },
                           'orders_changes': {'table_name': f'wb_orders{self.add_name}', 'uniq_columns': 'date, srid', 'partitions': 'warehouseName'},
                           'sales': {'table_name': f'wb_sales{self.add_name}', 'uniq_columns': 'date, saleID', 'partitions': 'warehouseName'},
                           'sales_changes': {'table_name':f'wb_sales{self.add_name}','uniq_columns':'date, saleID', 'partitions':'warehouseName' },
                           'realized': {'table_name':f'wb_realized{self.add_name}','uniq_columns':'realizationreport_id, rrd_id', 'partitions':'realizationreport_id'}}
                table_name = reports[report]['table_name']
                uniq_columns = reports[report]['uniq_columns']
                partitions = reports[report]['partitions']
                if report == 'orders': data = self.get_orders(date)
                if report == 'sales': data = self.get_sales(date)
                if report == 'orders_changes': data = self.get_orders_changes(date)
                if report == 'sales_changes': data = self.get_sales_changes(date)
                if report == 'realized': data = self.get_realized(date)
                self.clickhouse.create_alter_ch(data, table_name, uniq_columns, partitions, 'ReplacingMergeTree(timeStamp)')
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
            self.clickhouse.ch_insert(collection_data, f'wb_main_collection{self.add_name}')
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
            CREATE TABLE IF NOT EXISTS wb_main_collection{self.add_name} (
            date Date, report String, collect Bool ) ENGINE = ReplacingMergeTree(collect)
            ORDER BY (report, date)"""
        optimize_collection = f"OPTIMIZE TABLE wb_main_collection{self.add_name} FINAL"
        self.clickhouse.ch_execute(create_table_query_collect)
        self.clickhouse.ch_execute(optimize_collection)
        time.sleep(10)
        date_list = self.clickhouse.get_missing_dates(f'wb_main_collection{self.add_name}', report, self.start)
        for date in date_list:
            if self.err429 == False:
                print(f'Начинаем сбор. Репорт: {report}, Дата: {date}')
                logging.info(f'Начинаем сбор. Репорт: {report}, Дата: {date}')
                if datetime.strptime(date, '%Y-%m-%d').date() >= n_days_ago: collect = False
                else: collect = True
                collection_data = pd.DataFrame({'date': pd.to_datetime([date], format='%Y-%m-%d'), 'report': [report], 'collect': [collect]})
                self.upload_report(report, date, collection_data)
        if self.err429 == False:
            if report == 'orders' or report == 'sales':
                date = self.today.strftime('%Y-%m-%d')
                collection_data = pd.DataFrame({'date': [self.today], 'report': [f'{report}_changes'], 'collect': [True]})
                self.upload_report(f'{report}_changes', date, collection_data)

    def collecting_manager(self):
        report_list = self.reports.replace(' ','').lower().split(',')
        for report in report_list:
            if report in ['orders','sales','realized']:
                self.collecting_report(report)
            if report in ['reklama']:
                pass




