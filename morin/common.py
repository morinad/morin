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

    def shift_date(self, date_str, days=7):
        # Преобразуем строку в объект datetime
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        # Сдвигаем дату на указанное количество дней назад
        new_date = date_obj - timedelta(days=days)
        # Преобразуем дату обратно в строку
        return new_date.strftime('%Y-%m-%d')

    # значение -> тип значения для clickhouse
    def get_data_type(self, column, value, partitions):
        if value == None: return 'None'
        part_list = partitions.replace(' ', '').split(',')
        if isinstance(value, str):
            if value.lower() == 'false' or value.lower() == 'true':
                return 'UInt8'
            date_formats = [
                '%Y-%m-%dT%H:%M:%S',  # ISO формат DateTime: 2024-09-01T21:20:10
                '%Y-%m-%d %H:%M:%S',  # DateTime с пробелом: 2024-09-01 21:20:10
                '%Y-%m-%d',  # Формат Date: 2021-09-08
                '%d-%m-%Y',  # Формат Date с днем в начале: 08-09-2021
                '%Y/%m/%d',  # Формат Date через слэш: 2024/09/01
                '%H:%M:%S',  # Формат Time: 21:20:10
            ]
            for date_format in date_formats:
                try:
                    parsed_date = datetime.strptime(value, date_format)
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
                    continue  # Если строка не соответствует формату, продолжаем проверку
            try:
                parsed_date = parser.isoparse(value)
                return 'DateTime'  # Это формат DateTime с временной зоной
            except (ValueError, TypeError):
                pass
            try:
                if value.endswith('Z'):
                    value = value[:-1] + '+00:00'  # Заменяем 'Z' на '+00:00' для UTC
                if '+' in value and value[-3] == ':':
                    value = value[:-3] + value[-2:]  # Убираем двоеточие в зоне (+00:00 -> +0000)
                parsed_date = datetime.fromisoformat(value)
                return 'DateTime'  # Формат с датой, временем и временной зоной
            except ValueError:
                pass
            return 'String'  # Если ничего не подошло, возвращаем String

        # Если значение булевое
        elif isinstance(value, bool):
            return 'UInt8'

        # Если значение целое число
        elif isinstance(value, int):
            if len(str(abs(value))) > 10 or column in part_list:
                return 'String'
            return 'Float64'

        # Если значение с плавающей запятой
        elif isinstance(value, float):
            if math.isnan(value):
                return 'Float64'
            if len(str(int(abs(value)))) > 10 or column in part_list:
                return 'String'
            return 'Float64'

        # Для всех остальных типов
        else:
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

    def analyze_column_types(self, data, uniq_columns, partitions):
        try:
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
                if len(types) == 1:
                    final_column_types[column] = next(iter(types))
                else:
                    final_column_types[column] = 'String'  # Если разные типы, делаем строкой
            create_table_query = []
            non_nullable_list = uniq_columns.replace(' ','').split(',')+[partitions.strip()]
            for field, data_type in final_column_types.items():
                field_type = f'Nullable({data_type})'
                for non in non_nullable_list:
                    if field == non:
                        field_type = f'{data_type}'
                create_table_query.append(f"{field} {field_type}")
        except Exception as e:
            print(f'Ошибка анализа: {e}')
            logging.info(f'Ошибка анализа: {e}')
        return create_table_query

    # список словарей (данные) -> датафрейм с нужными типами
    def check_and_convert_types(self, data, uniq_columns, partitions):
        try:
            columns_list=self.analyze_column_types(data, uniq_columns, partitions)
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
                            df[column_name] = pd.to_datetime(df[column_name], errors='raise')
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
