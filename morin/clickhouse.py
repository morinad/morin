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
    def __init__(self, logging_path:str, host: str, port: str, username: str, password: str, database: str, start:str, add_name:str, err429:bool, backfill_days:int, platform:str):
        self.logging_path = logging_path
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.now = datetime.now()
        self.start = start
        self.add_name = add_name
        self.err429 = err429
        self.backfill_days = backfill_days
        self.today = datetime.now().date()
        self.platform = platform
        self.common = Common(logging_path)
        logging.basicConfig(filename=self.logging_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



    def test_clickhouse_connection(self):
        try:
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database
            )
            client.command('SELECT 1')
            print("Подключение к ClickHouse успешно!")
            logging.info("Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            print(f"Ошибка подключения к ClickHouse: {e}")
            logging.info(f"Ошибка подключения к ClickHouse: {e}")
            return False


    def convert_column_to_text(self, client, table_name, column_name, column_type):
        client.command(f"""ALTER TABLE {table_name} ADD COLUMN test {column_type};""")
        client.command(f"""ALTER TABLE {table_name} UPDATE test = toString({column_name}) WHERE 1;""")
        time.sleep(3)
        client.command(f"OPTIMIZE TABLE {table_name} FINAL")
        time.sleep(3)
        client.command(f"""ALTER TABLE {table_name} DROP COLUMN {column_name};""")
        client.command(f"""ALTER TABLE {table_name} RENAME COLUMN test TO {column_name};""")

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

    def ch_text_columns_set(self, table_name):
        client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username,
                                               password=self.password, database=self.database)
        text_columns_set = set()
        try:
            query = f"DESCRIBE TABLE {table_name};"
            result = client.query(query)
            columns_info = result.result_rows
            for col in columns_info:
                elem = f"{col[0]} {col[1]}"
                if 'String' in elem:
                    text_columns_set.add(f"{col[0].strip()}")
        except:
            pass
        return text_columns_set

    # список словарей (данные)+уникальность+имятаблицы -> создание/изменение таблицы ch
    def create_alter_ch(self, data, table_name, uniq_columns, partitions, mergetree):
        try:
            text_columns_set = self.ch_text_columns_set(table_name)
            upload_list = self.common.analyze_column_types(data, uniq_columns, partitions, text_columns_set)
            upload_set = set()
            uploads = ''
            for i in upload_list:
                if 'None' not in i:
                    upload_set.add(i)
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
            current_names_set = set([f"{col[0].strip()}" for col in columns_info])
            diff = list(upload_set - current_set)
            if len(diff) > 0:
                start_alter_exp=f'ALTER TABLE {table_name} '
                for d in diff:
                    column_name = d.split(' ')[0].strip()
                    column_type = d.split(' ')[1].strip()
                    if column_name in current_names_set and 'String' in column_type:
                        self.convert_column_to_text(client, table_name, column_name, column_type)
                        alter_exp = f"преобразуем столбец {column_name} в текст"
                    else:
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


    def upload_data(self, platform, report_name,upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date):
        if self.err429 == False:
            try:
                n_days_ago = self.today - timedelta(days=self.backfill_days)
                table_name = f'{platform}_{upload_table}_{self.add_name}'
                text_columns_set = self.ch_text_columns_set(table_name)
                if refresh_type == 'delete_date':
                    refresh = f"ALTER TABLE {table_name} DROP PARTITION '{date}';"
                elif refresh_type == 'delete_all':
                    refresh = f"TRUNCATE TABLE {table_name};"
                else:
                    refresh = f"OPTIMIZE TABLE {table_name};"
                data = func_name(date)
                collect = True
                if history and datetime.strptime(date, '%Y-%m-%d').date() >= n_days_ago:
                    collect = False
                collection_data = pd.DataFrame({'date': pd.to_datetime([date], format='%Y-%m-%d'), 'report': [report_name], 'collect': [collect]})
                self.create_alter_ch(data, table_name, uniq_columns, partitions, merge_type)
                df = self.common.check_and_convert_types(data, uniq_columns, partitions, text_columns_set)
                self.ch_execute(refresh)
                self.ch_insert(df, table_name)
                self.ch_insert(collection_data, f'{platform}_collection_{self.add_name}')
                print(f'Данные добавлены. Репорт: {report_name}. Дата: {date}')
                logging.info(f'Данные добавлены. Репорт: {report_name}. Дата: {date}')
                time.sleep(delay)
            except Exception as e:
                print(f'Ошибка вставки: {e}. Репорт: {report_name}. Дата: {date}')
                logging.info(f'Ошибка вставки: {e}. Репорт: {report_name}. Дата: {date}')
                time.sleep(delay)
        else:
            raise ValueError("Обнаружена ошибка 429")


    def collecting_report(self, platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, frequency, delay):
        self.test_clickhouse_connection()
        logging.info(f"Начинаем сбор {report_name} для клиента: {self.add_name}")
        print(f"Начинаем сбор {report_name} для клиента: {self.add_name}")
        create_table_query_collect = f"""
            CREATE TABLE IF NOT EXISTS {platform}_collection_{self.add_name} (
            date Date, report String, collect Bool ) ENGINE = ReplacingMergeTree(collect) ORDER BY (report, date)"""
        optimize_collection = f"OPTIMIZE TABLE {platform}_collection_{self.add_name} FINAL"
        self.ch_execute(create_table_query_collect)
        self.ch_execute(optimize_collection)
        time.sleep(3)
        if history:
            date_list = self.get_missing_dates(f'{platform}_collection_{self.add_name}', report_name, self.start)
            for date in date_list:
                if self.err429 == False and self.common.to_collect(frequency, date):
                    print(f'Начинаем сбор. Репорт: {report_name}, Дата: {date}')
                    logging.info(f'Начинаем сбор. Репорт: {report_name}, Дата: {date}')
                    self.upload_data(platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date)
        else:
            date = self.today.strftime('%Y-%m-%d')
            if self.err429 == False and self.common.to_collect(frequency, date):
                print(f'Начинаем сбор. Репорт: {report_name}, Дата: {date}')
                logging.info(f'Начинаем сбор. Репорт: {report_name}, Дата: {date}')
                self.upload_data(platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date)