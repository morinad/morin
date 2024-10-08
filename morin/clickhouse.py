from .common import Common
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

class Clickhouse:
    def __init__(self, logging_path:str, host: str, port: str, username: str, password: str, database: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.common = Common(logging_path)
        logging.basicConfig(filename=logging_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # датафрейм, название таблицы -> вставка данных
    def ch_insert(self, df, to_table):
        try:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username,
                                                   password=self.password, database=self.database)
            client.insert(to_table, data_tuples, column_names=df.columns.tolist())
            print(f'Данные вставлены в CH, таблица {to_table}')
            logging.info(f'Данные вставлены в CH, таблица {to_table}')
            optimize_table = f"OPTIMIZE TABLE {to_table} FINAL"
            client.command(optimize_table)
        except Exception as e:
            print(f'Ошибка вставки в CH: {e}')
            logging.info(f'Ошибка вставки в CH: {e}')
            raise
        finally:
            if client:
                client.close()

    def ch_execute(self, expression):
        try:
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
            client.command(expression)
            disp_exp = expression.strip()[:17]+'...'
            print(f'Выражение {disp_exp} выполнено')
            logging.info(f'Выражение {disp_exp} выполнено')
        except Exception as e:
            print(f'Ошибка выражения {disp_exp}: {e}')
            logging.info(f'Ошибка выражения {disp_exp}: {e}')
        finally:
            if client:
                client.close()

    # список словарей (данные)+уникальность+имятаблицы -> создание/изменение таблицы ch
    def create_alter_ch(self, data, table_name, uniq_columns, partitions, mergetree):
        try:
            upload_list = self.common.analyze_column_types(data, uniq_columns, partitions)
            upload_set = set(upload_list)
            uploads = ''
            for i in upload_list:
                uploads += i + ',\n'
            if partitions == '':
                part_part =''
            else:
                part_part = f'PARTITION BY {partitions}'
            create_table_query_campaigns = f'CREATE TABLE IF NOT EXISTS {table_name} (' + uploads + f'timeStamp DateTime ) ENGINE = {mergetree} ORDER BY ({uniq_columns}) {part_part}'
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
            client.query(create_table_query_campaigns)
            query = f"DESCRIBE TABLE {table_name};"
            result = client.query(query)
            columns_info = result.result_rows
            current_set = set([f"{col[0]} {col[1]}" for col in columns_info])
            diff = list(upload_set - current_set)
            if len(diff) > 0:
                start_alter_exp=f'ALTER TABLE {table_name} '
                for d in diff:
                    alter_exp =start_alter_exp + 'ADD COLUMN IF NOT EXISTS ' + d + ' AFTER timeStamp;'
                    print(f'Попытка изменения {table_name}, Формула: {alter_exp}')
                    logging.info(f'Попытка изменения {table_name}, Формула: {alter_exp}')
                    client.query(alter_exp)
                    print(f'Успешное изменение {table_name}, Формула: {alter_exp}')
                    logging.info(f'Успешное изменение {table_name}, Формула: {alter_exp}')
                    time.sleep(2)
            else:
                print(f'Данные готовы для вставки в {table_name}')
                logging.info(f'Данные готовы для вставки в {table_name}')
        except Exception as e:
            print(f'Ошибка подготовки данных: {e}')
            logging.info(f'Ошибка подготовки данных: {e}')


    def get_missing_dates(self, table_name, report_name, start_date_str):
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            # Получение всех существующих дат для заданного report
            query = f"""
            SELECT date
            FROM {table_name}
            WHERE report = '{report_name}' and collect = True"""
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
            result = client.query(query)
            existing_dates = {row[0] for row in result.result_rows}
            current_date = start_date
            all_dates = set()
            while current_date < self.today:
                all_dates.add(current_date)
                current_date += timedelta(days=1)
            missing_dates = sorted(all_dates - existing_dates)
            missing_dates_str = [date.strftime('%Y-%m-%d') for date in missing_dates]
            print(f'Успешное получение дат. Таблица: {table_name}, Старт: {start_date}')
            logging.info(f'Успешное получение дат. Таблица: {table_name}, Старт: {start_date}')
        except Exception as e:
            print(f'Ошибка получения дат: {e}')
            logging.info(f'Ошибка получения дат: {e}')
        return missing_dates_str



