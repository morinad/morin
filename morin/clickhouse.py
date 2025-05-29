from .common import Common
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

class Clickhouse:
    def __init__(self,  bot_token:str, chat_list:str, message_type: str, host: str, port: str, username: str, password: str, database: str, start:str, add_name:str, err429:bool, backfill_days:int, platform:str):
        self.bot_token = bot_token
        self.chat_list = chat_list
        self.message_type = message_type
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
        self.common = Common(self.bot_token, self.chat_list, self.message_type)



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
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Подключение к ClickHouse успешно!'
            try:
                self.common.log_func(self.bot_token, self.chat_list, message,1)
            except:
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return True
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Ошибка подключения к ClickHouse: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message,3)
            return False


    def convert_column_to_text(self, client, table_name, column_name, column_type):
        client.command(f"""ALTER TABLE {table_name} ADD COLUMN test1 {column_type};""")
        print(f"Создан столбец test1: {table_name}, {column_name}, {column_type}")
        client.command(f"""ALTER TABLE {table_name} UPDATE test1 = toString({column_name}) WHERE 1;""")
        time.sleep(5)
        client.command(f"OPTIMIZE TABLE {table_name} FINAL")
        time.sleep(5)
        client.command(f"""ALTER TABLE {table_name} DROP COLUMN {column_name};""")
        client.command(f"""ALTER TABLE {table_name} RENAME COLUMN test1 TO {column_name};""")

    def convert_column_to_date(self, client, table_name, column_name):
        client.command(f"""ALTER TABLE {table_name} ADD COLUMN test2 Date;""")
        print(f"Создан столбец test2: {table_name}, {column_name}")
        client.command(f"""ALTER TABLE {table_name} UPDATE test2 = toDate({column_name}) WHERE 1;""")
        time.sleep(5)
        client.command(f"OPTIMIZE TABLE {table_name} FINAL;")
        time.sleep(5)
        client.command(f"""ALTER TABLE {table_name} DROP COLUMN {column_name};""")
        client.command(f"""ALTER TABLE {table_name} RENAME COLUMN test2 TO {column_name};""")

    # датафрейм, название таблицы -> вставка данных
    import numpy as np



    def ch_insert(self, df, to_table,chunk_size=20000):
        try:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username,
                                                   password=self.password, database=self.database)

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                data_tuples = [tuple(x) for x in chunk.to_numpy()]
                client.insert(to_table, data_tuples, column_names=chunk.columns.tolist())
                time.sleep(1)

            message =f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {to_table}. Результат: данные вставлены в CH!'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            optimize_table = f"OPTIMIZE TABLE {to_table} FINAL"
            client.command(optimize_table)
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {to_table}. Функция: ch_insert. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise
        finally:
            if client:
                client.close()

    def ch_execute(self, expression):
        client = None
        try:
            disp_exp = expression.strip()[:30] + '...'
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
            client.command(expression)
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Выражение {disp_exp} выполнено.'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Ошибка выражения {disp_exp}: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
        finally:
            if client:
                client.close()

    def ch_check(self, table_name):
        try:
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
            result = client.command(f'EXISTS TABLE {table_name}')
            print(result)
            if result == 1:
                print(f'Таблица {table_name} существует.')
                return True
            else:
                print(f'Таблица {table_name} не существует.')
                return False
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
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
            dangerous_columns_set = set(uniq_columns.strip().split(',') + partitions.strip().split(','))
            print(table_name)
            text_columns_set = self.ch_text_columns_set(table_name)
            upload_list = self.common.analyze_column_types(data, uniq_columns, partitions, text_columns_set)
            upload_set = set()
            print('upload_list',upload_list)
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
            # print(create_table_query_campaigns)
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
            client.query(create_table_query_campaigns)
            query = f"DESCRIBE TABLE {table_name};"
            result = client.query(query)
            columns_info = result.result_rows
            current_set = set([f"{col[0]} {col[1]}" for col in columns_info])
            print(current_set)
            current_names_set = set([f"{col[0].strip()}" for col in columns_info])
            diff = list(upload_set - current_set)
            if len(diff) > 0:
                start_alter_exp=f'ALTER TABLE {table_name} '
                for d in diff:
                    column_name = d.split(' ')[0].strip()
                    column_type = d.split(' ')[1].strip()
                    if column_name in current_names_set and 'String' in column_type:
                        message = f'Платформа: {self.platform}. Имя: {self.add_name}. Приведение к тексту {column_name} в {table_name}.'
                        self.common.log_func(self.bot_token, self.chat_list, message, 1)
                        if column_name not in dangerous_columns_set:
                            self.convert_column_to_text(client, table_name, column_name, column_type)
                            alter_exp = f"преобразуем столбец {column_name} в текст"
                    elif column_name in current_names_set and column_type == 'Date':
                        message = f'Платформа: {self.platform}. Имя: {self.add_name}. Приведение к дате {column_name} в {table_name}.'
                        self.common.log_func(self.bot_token, self.chat_list, message, 1)
                        if column_name not in dangerous_columns_set:
                            self.convert_column_to_date(client, table_name, column_name)
                            alter_exp = f"преобразуем столбец {column_name} в дату"
                    else:
                        alter_exp =start_alter_exp + 'ADD COLUMN IF NOT EXISTS ' + d + ' AFTER timeStamp;'
                        message = f'Платформа: {self.platform}. Имя: {self.add_name}. Попытка изменения {table_name}. Формула: {alter_exp}'
                        self.common.log_func(self.bot_token, self.chat_list, message, 1)
                        client.query(alter_exp)
                    message = f'Платформа: {self.platform}. Имя: {self.add_name}. Успешное изменение {table_name}. Формула: {alter_exp}'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                    time.sleep(2)
            else:
                message = f'Платформа: {self.platform}. Имя: {self.add_name}. Данные готовы для вставки в {table_name}'
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Функция: create_alter_ch. Ошибка подготовки данных: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)



    def get_missing_dates(self, table_name, report_name, start_date_str, include_today):
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
            if include_today:
                while current_date <= self.today:
                    all_dates.add(current_date)
                    current_date += timedelta(days=1)
            else:
                while current_date < self.today:
                    all_dates.add(current_date)
                    current_date += timedelta(days=1)
            missing_dates = sorted(all_dates - existing_dates)
            missing_dates_str = [date.strftime('%Y-%m-%d') for date in missing_dates]
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Старт: {start_date}. Функция: get_missing_dates. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return missing_dates_str
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Функция: get_missing_dates. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None



    def upload_data(self, platform, report_name,upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date):
        try:
            if self.err429 == False:
                n_days_ago = self.today - timedelta(days=self.backfill_days)
                table_name = f'{platform}_{upload_table}_{self.add_name}'

                text_columns_set = self.ch_text_columns_set(table_name)
                if refresh_type == 'delete_date':
                    refresh = f"ALTER TABLE {table_name} DROP PARTITION '{date}';"
                elif refresh_type == 'delete_all':
                    refresh = f"TRUNCATE TABLE {table_name};"
                else:
                    refresh = f"OPTIMIZE TABLE {table_name};"
                print(refresh)
                data = func_name(date)
                if not self.common.is_error(data):
                    collect = True
                    if history and datetime.strptime(date, '%Y-%m-%d').date() >= n_days_ago:
                        collect = False
                    collection_data = pd.DataFrame({'date': pd.to_datetime([date], format='%Y-%m-%d'), 'report': [report_name], 'collect': [collect]})
                    if self.common.is_empty(data):
                        message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Дата: {date}. ПУСТОЙ ОТВЕТ!'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)
                    if not self.common.is_empty(data):
                        self.create_alter_ch(data, table_name, uniq_columns, partitions, merge_type)
                        df = self.common.check_and_convert_types(data, uniq_columns, partitions, text_columns_set)
                    if self.ch_check(table_name):
                        self.ch_execute(refresh)
                    if not self.common.is_empty(data):
                        self.ch_insert(df, table_name)
                    self.ch_insert(collection_data, f'{platform}_collection_{self.add_name}')
                    message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Дата: {date}. Данные добавлены!'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                time.sleep(delay)
            else:
                message = f'Платформа: {platform}. Имя: {self.add_name}. Таблица: {report_name}. Функция: upload_data. Ошибка: 429.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                raise ValueError("Обнаружена ошибка 429")
        except Exception as e:
            message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Дата: {date}. Ошибка вставки: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            time.sleep(delay)



    def collecting_report(self, platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, frequency, delay):
        try:
            self.test_clickhouse_connection()
            create_table_query_collect = f"""
                CREATE TABLE IF NOT EXISTS {platform}_collection_{self.add_name} (
                date Date, report String, collect Bool ) ENGINE = ReplacingMergeTree(collect) ORDER BY (report, date)"""
            optimize_collection = f"OPTIMIZE TABLE {platform}_collection_{self.add_name} FINAL"
            self.ch_execute(create_table_query_collect)
            self.ch_execute(optimize_collection)
            time.sleep(4)
            if history:
                date_list = self.get_missing_dates(f'{platform}_collection_{self.add_name}', report_name, self.start,  False)
                for date in date_list:
                    if self.err429 == False and self.common.to_collect(frequency, date):
                        message = f'Платформа: {platform}. Имя: {self.add_name}. Таблица: {upload_table}. Репорт: {report_name}. Дата: {date}. Начинаем сбор...'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)
                        self.upload_data(platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date)
            else:
                date = self.today.strftime('%Y-%m-%d')
                if self.err429 == False and self.common.to_collect(frequency, date):
                    message = f'Платформа: {platform}. Имя: {self.add_name}. Таблица: {upload_table}. Репорт: {report_name}. Дата: {date}. Начинаем сбор...'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                    self.upload_data(platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date)
        except Exception as e:
            message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Функция: collecting_report. Ошибка сбора: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
