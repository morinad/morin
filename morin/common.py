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
import chardet
import json
import math
from transliterate import translit

class Common:
    def __init__(self, logging_path:str):
        self.logging_path = logging_path
        self.now = datetime.now()
        self.today = datetime.now().date()
        logging.basicConfig(filename=self.logging_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    def get_data_type(self, column, value, partitions):
        value = str(value)
        part_list = partitions.replace(' ', '').split(',')
        if value == None or value.strip() == '': return 'None'
        if value.lower() == 'false' or value.lower() == 'true':
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
                parsed_date = datetime.strptime(value.replace('Z', ''), date_format)
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
            float_value = float(value)
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
                for column, value in row.items():
                    value_type = self.get_data_type(column, value, partitions)  # Определяем тип данных
                    if column not in column_types:
                        column_types[column] = set()  # Создаем множество для уникальных типов
                    column_types[column].add(value_type)
            # Приводим типы столбцов к общему типу
            final_column_types = {}
            for column, types in column_types.items():
                try: types.remove('None')
                except: pass
                print(column, types)
                if len(types) == 1 and column.strip() not in  text_columns_set:
                    final_column_types[column] = next(iter(types))
                elif len(types) == 0:
                    final_column_types[column] = 'None'
                    print('выдан none')
                else:
                    final_column_types[column] = 'String'  # Если разные типы, делаем строкой
            create_table_query = []
            non_nullable_list = uniq_columns.replace(' ','').split(',')+[partitions.strip()]
            for field, data_type in final_column_types.items():
                if data_type != 'None':
                    field_type = f'Nullable({data_type})'
                elif data_type == 'None':
                    field_type = 'None'
                for non in non_nullable_list:
                    if field == non:
                        field_type = f'{data_type}'
                create_table_query.append(f"{field} {field_type}")
        except Exception as e:
            print(f'Ошибка анализа: {e}')
            logging.info(f'Ошибка анализа: {e}')
        print(create_table_query)
        return create_table_query

    # список словарей (данные) -> датафрейм с нужными типами
    def check_and_convert_types(self, data, uniq_columns, partitions, text_columns_set):
        try:
            columns_list=self.analyze_column_types(data, uniq_columns, partitions,text_columns_set)
            df=pd.DataFrame(data,dtype=str)
            type_mapping = {
                'UInt8': 'bool',
                'Nullable(UInt8)': 'bool',
                'Date': 'datetime64[ns]',  # pandas формат для дат
                'DateTime': 'datetime64[ns]',  # pandas формат для дат с временем
                'String': 'object',  # Строковый формат в pandas
                'Float64': 'float64',  # float64 тип в pandas
                'Nullable(Date)': 'datetime64[ns]',  # pandas формат для дат
                'Nullable(DateTime)': 'datetime64[ns]',  # pandas формат для дат с временем
                'Nullable(String)': 'object',  # Строковый формат в pandas
                'Nullable(Float64)': 'float64'  # float64 тип в pandas
            }
            for item in columns_list:
                column_name, expected_type = item.split()  # Разделяем по пробелу: 'column_name expected_type'
                if column_name in df.columns:
                    expected_type = expected_type.strip()
                    try:
                        if expected_type in ['Date', 'Nullable(Date)']:
                            df[column_name] = df[column_name].apply(self.column_to_datetime)
                            df[column_name] = pd.to_datetime(df[column_name], errors='raise').dt.date
                            df[column_name] = df[column_name].fillna(pd.to_datetime('1970-01-01').date())
                        if expected_type in ['DateTime', 'Nullable(DateTime)']:
                            df[column_name] = df[column_name].apply(self.column_to_datetime)
                            df[column_name] = pd.to_datetime(df[column_name], errors='raise')
                            df[column_name] = df[column_name].fillna(pd.Timestamp('1970-01-01'))
                        elif expected_type in ['UInt8','Nullable(UInt8)']:
                            df[column_name] = df[column_name].replace({'True': True, 'False': False, 'true': True, 'false': False, })
                            df[column_name] = df[column_name].fillna(False)
                            df[column_name] = df[column_name].astype('bool')
                        elif expected_type in ['Float64','Nullable(Float64)']:
                            df[column_name] = pd.to_numeric(df[column_name], errors='raise').astype('float64')
                            df[column_name] = df[column_name].fillna(0)
                        elif expected_type in ['String','Nullable(String)']:
                            df[column_name] = df[column_name].astype(str)
                            df[column_name] = df[column_name].fillna("")
                        elif 'None' in expected_type:
                            df = df.drop(columns=[column_name])
                            print(column_name)
                    except Exception as e:
                        print(f"Ошибка при преобразовании столбца '{column_name}': {e}")
                        logging.info(f"Ошибка при преобразовании столбца '{column_name}': {e}")
            df['timeStamp'] = self.now
            print(f'Датафрейм успешно преобразован')
            logging.info(f'Датафрейм успешно преобразован')
        except Exception as e:
            print(f'Ошибка преобразования df: {e}')
            logging.info(f'Ошибка преобразования df: {e}')
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
