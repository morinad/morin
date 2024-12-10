import requests
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
import os
from dateutil import parser
import time
import hashlib
from io import StringIO
import chardet
import json
import math
from transliterate import translit

class Common:
    def __init__(self, bot_token:str, chat_list:str, message_type: str):
        self.bot_token = bot_token
        self.chat_list = chat_list

        if message_type == 'all':
            self.value = 1
        elif message_type == 'key':
            self.value = 2
        else:
            self.value = 3
        self.now = datetime.now()
        self.today = datetime.now().date()

    def log_func(self, bot_token, chat_ids,message, value):
        print(message)
        log_file_path = "/app/log.txt"
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        if value >= self.value:
            with open(log_file_path, "a") as log_file:
                log_file.write(message + "\n\n")
        self.send_logs_clear(bot_token, chat_ids, message)

    def send_logs_clear(self,bot_token, chat_ids, message):
        log_file_path = "/app/log.txt"
        if not os.path.exists(log_file_path):
            print("Файл лога не существует.")
        with open(log_file_path, "r") as log_file:
            content = log_file.read()
        if len(content) > 1000:
            self.message_text = content
            self.send_logs(bot_token, chat_ids)
            with open(log_file_path, "w") as log_file:
                log_file.write("")  # Очищаем файл
            print("Файл очищен, длина содержимого превышала 1000 символов.")
        return content

    def send_logs(self, bot_token, chat_ids):
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        for chat_id in chat_ids:
            try:
                params = {'chat_id': chat_id, 'text': self.message_text}
                response = requests.get(url, params=params)
                if response.status_code != 200:
                    print(f"Ошибка отправки сообщения в чат {chat_id}: {response.text}")
            except:
                print(f"Ошибка отправки сообщения в чат {chat_id}: {response.text}")
        self.message_text = ''

    def is_empty(self, result):
        if not result:
            return True
        if isinstance(result, list) and all(isinstance(item, dict) and not item for item in result):
            return True
        return False

    def is_error(self, result):
        if isinstance(result, str):
            if 'Ошибка:' in result:
                return True
        return False

    def shorten_text(self, text):
        # Используем хеш-функцию md5 для сокращения строки
        hash_object = hashlib.md5(text.encode())  # Можно также использовать sha256
        return hash_object.hexdigest()[:10]  # Возвращаем первые 10 символов хеша

    def transliterate_key(self, key):
        tr = translit(key, 'ru', reversed=True)
        tr = tr.strip().replace(' ', '_').replace('-', '_').replace(",", '').replace("'", '').replace(".", '').replace("(",'').replace(")", '').lower()
        return  tr

    def shift_date(self, date_str, days=7):
        # Преобразуем строку в объект datetime
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        # Сдвигаем дату на указанное количество дней назад
        new_date = date_obj - timedelta(days=days)
        # Преобразуем дату обратно в строку
        return new_date.strftime('%Y-%m-%d')


    def keep_last_20000_lines(self,file_path):
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
        with open(file_path, 'r', encoding=encoding) as file:
            lines = file.readlines()
        last_20000_lines = lines[-20000:]
        with open(file_path, 'w', encoding=encoding) as file:
            file.writelines(last_20000_lines)

    # значение -> тип значения для clickhouse
    def get_data_type(self, column, getvalue, partitions):
        getvalue = str(getvalue)
        part_list = partitions.replace(' ', '').split(',')
        if getvalue == None or getvalue.strip() == '': return 'None'
        if getvalue.lower() == 'false' or getvalue.lower() == 'true':
            return 'UInt8'
        date_formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",  # 2023-10-22T16:36:15.507+0000
            "%Y-%m-%d %H:%M:%S.%f%z",  # 2023-10-22 16:36:15.507+0000
            "%Y-%m-%dT%H:%M:%S%z",  # 2023-10-22T16:36:15+0000
            "%Y-%m-%d %H:%M:%S%z",  # 2023-10-22 16:36:15+0000
            "%Y-%m-%dT%H:%M:%S.%f",  # 2023-10-22T16:36:15.507 (без таймзоны)
            "%Y-%m-%d %H:%M:%S.%f",  # 2023-10-22 16:36:15.507 (без 'T')
            "%Y-%m-%dT%H:%M:%S",  # 2023-10-22T16:36:15 (без миллисекунд и таймзоны)
            "%Y-%m-%d %H:%M:%S",  # 2023-10-22 16:36:15 (без 'T', без миллисекунд)
            "%Y-%m-%d",  # 2023-10-22 (только дата)
            "%d-%m-%Y",  # 22-10-2023 (европейский формат)  # Формат Date с днем в начале: 08-09-2021
            '%Y/%m/%d',  # Формат Date через слэш: 2024/09/01
            '%H:%M:%S',  # Формат Time: 21:20:10
        ]
        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(getvalue.replace('Z', ''), date_format)
                # Если дата меньше 1970 года — это не допустимая дата для ClickHouse
                if parsed_date.year < 1970:
                    return 'String'
                # Определяем тип на основе формата
                if date_format in ['%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d']:
                    return 'Date'  # Это формат Date
                elif date_format == '%H:%M:%S':
                    return 'Time'  # Это формат Time
                else:
                    return 'DateTime'  # Форматы с датой и временем
            except ValueError:
                continue
        try:
            float_value = float(getvalue)
            if len(str(float_value)) < 15 and column not in part_list:
                return 'Float64'
        except:
            pass
        return 'String'


    def column_to_datetime(self, date_str):
        if pd.isna(date_str):
            return None
        date_str = date_str.strip()

        # Обрабатываем таймзону 'Z' (UTC) и заменяем на '+0000'
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+0000'
        # Обрабатываем таймзоны вида +00:00 и заменяем на +0000
        elif '+' in date_str and date_str.endswith(':00'):
            date_str = date_str[:-3] + date_str[-2:]

        date_formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",  # 2023-10-22T16:36:15.507+0000
            "%Y-%m-%d %H:%M:%S.%f%z",  # 2023-10-22 16:36:15.507+0000
            "%Y-%m-%dT%H:%M:%S%z",  # 2023-10-22T16:36:15+0000
            "%Y-%m-%d %H:%M:%S%z",  # 2023-10-22 16:36:15+0000
            "%Y-%m-%dT%H:%M:%S.%f",  # 2023-10-22T16:36:15.507 (без таймзоны)
            "%Y-%m-%d %H:%M:%S.%f",  # 2023-10-22 16:36:15.507 (без 'T')
            "%Y-%m-%dT%H:%M:%S",  # 2023-10-22T16:36:15 (без миллисекунд и таймзоны)
            "%Y-%m-%d %H:%M:%S",  # 2023-10-22 16:36:15 (без 'T', без миллисекунд)
            "%Y-%m-%d",  # 2023-10-22 (только дата)
            "%d-%m-%Y"  # 22-10-2023 (европейский формат)
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                continue
        return None
        # список словарей (данные) -> список поле_типданных

    def analyze_column_types(self, data, uniq_columns, partitions, text_columns_set):
        try:
            null_columns = []
            column_types = {}
            # Проходим по всем строкам в данных
            for row in data:
                for column, anvalue in row.items():
                    value_type = self.get_data_type(column, anvalue, partitions)  # Определяем тип данных
                    if column not in column_types:
                        column_types[column] = set()  # Создаем множество для уникальных типов
                    column_types[column].add(value_type)
            # Приводим типы столбцов к общему типу
            final_column_types = {}
            for column, types in column_types.items():
                try: types.remove('None')
                except: pass
                if len(types) == 1 and column.strip() not in  text_columns_set:
                    final_column_types[column] = next(iter(types))
                elif len(types) == 0:
                    final_column_types[column] = 'None'
                else:
                    final_column_types[column] = 'String'  # Если разные типы, делаем строкой
            create_table_query = []
            # non_nullable_list = uniq_columns.replace(' ','').split(',')+[partitions.strip()]
            for field, data_type in final_column_types.items():
                create_table_query.append(f"{field} {data_type}")
        except Exception as e:
            message = f'Функция: analyze_column_types. Ошибка: {e}'
            self.log_func(self.bot_token, self.chat_list,message, 3)


        return create_table_query

    # список словарей (данные) -> датафрейм с нужными типами
    def check_and_convert_types(self, data, uniq_columns, partitions, text_columns_set):
        try:
            columns_list=self.analyze_column_types(data, uniq_columns, partitions,text_columns_set)
            df=pd.DataFrame(data,dtype=str)
            type_mapping = {
                'UInt8': 'bool',
                'Date': 'datetime64[ns]',  # pandas формат для дат
                'DateTime': 'datetime64[ns]',  # pandas формат для дат с временем
                'String': 'object',  # Строковый формат в pandas
                'Float64': 'float64',  # float64 тип в pandas
            }
            for item in columns_list:
                column_name, expected_type = item.split()  # Разделяем по пробелу: 'column_name expected_type'
                if column_name in df.columns:
                    expected_type = expected_type.strip()
                    try:
                        if expected_type in ['Date']:
                            df[column_name] = df[column_name].apply(self.column_to_datetime)
                            df[column_name] = pd.to_datetime(df[column_name], errors='raise').dt.date
                            df[column_name] = df[column_name].fillna(pd.to_datetime('1970-01-01').date())
                        if expected_type in ['DateTime']:
                            df[column_name] = df[column_name].apply(self.column_to_datetime)
                            df[column_name] = pd.to_datetime(df[column_name], errors='raise')
                            df[column_name] = df[column_name].fillna(pd.Timestamp('1970-01-01'))
                        elif expected_type in ['UInt8']:
                            df[column_name] = df[column_name].replace({'True': True, 'False': False, 'true': True, 'false': False, })
                            df[column_name] = df[column_name].fillna(False)
                            df[column_name] = df[column_name].astype('bool')
                        elif expected_type in ['Float64']:
                            df[column_name] = pd.to_numeric(df[column_name], errors='raise').astype('float64')
                            df[column_name] = df[column_name].fillna(0)
                        elif expected_type in ['String']:
                            df[column_name] = df[column_name].astype(str)
                            df[column_name] = df[column_name].fillna("")
                        elif 'None' in expected_type:
                            df = df.drop(columns=[column_name])
                    except Exception as e:
                        message = f"Функция: check_and_convert_types. Ошибка при преобразовании столбца '{column_name}': {e}"
                        self.log_func(self.bot_token, self.chat_list, message, 3)
            df['timeStamp'] = self.now
            message = f'Функция: check_and_convert_types. Датафрейм успешно преобразован'
            self.log_func(self.bot_token, self.chat_list, message, 2)
        except Exception as e:
            message= f'Функция: check_and_convert_types. Ошибка преобразования df: {e}'
            self.log_func(self.bot_token, self.chat_list, message, 3)
        return df

    def to_collect(self, schedule_str, date_str):
        try:
            today = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            raise ValueError("Дата должна быть в формате 'YYYY-MM-DD'")
        day_of_week = today.strftime('%A').lower()  # День недели (например, 'friday')
        day_of_month = today.day  # Число месяца (например, 22)
        schedule_list = [s.strip().lower() for s in schedule_str.split(',')]
        for schedule in schedule_list:
            if schedule == 'daily':  # Если указано "daily", всегда возвращаем True
                return True
            if schedule == day_of_week:  # Проверка дня недели (например, 'friday')
                return True
            if schedule.isdigit() and int(schedule) == day_of_month:  # Проверка числа месяца
                return True
        return False

    def spread_table(self, source_list):
        result_list = []
        for row in source_list:
            row_dict = {}
            for key, value in row.items():
                if isinstance(value, dict):
                    for name, inner_value in dict(value).items():
                        row_dict[f'{key}_{name}'] = inner_value
                else:
                    row_dict[f'{key}'] = value
            result_list.append(row_dict)
        return result_list
