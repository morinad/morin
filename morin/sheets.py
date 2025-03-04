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
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

class Sheets:
    def __init__(self,
                 sheets_client_id = "94751375714-8fl0tna32fj28g7bsignpkgi0upip55b.apps.googleusercontent.com",
                 sheets_client_secret  = "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",
                 sheets_redirect_uri = "http://127.0.0.1:9004"
                 ):
        self.sheets_client_id = sheets_client_id
        self.sheets_client_secret = sheets_client_secret
        self.sheets_redirect_uri = sheets_redirect_uri

    def str_value(self, value):
        try:
            # Пробуем преобразовать значение в float
            float_value = float(value)

            # Проверяем, является ли число целым (дробная часть нулевая)
            if float_value == int(float_value):
                return int(float_value)  # Возвращаем целое число как строку
            else:
                # Возвращаем число с запятой вместо точки
                return (str(float_value).replace('.', ','))
        except (ValueError, TypeError):
            # Если не удалось преобразовать в число, возвращаем как строку
            return str(value)


    def generate_dates_list(self,days_back, include_today=False):
        date_list = []
        today = datetime.today()
        end_date = today if include_today else today - timedelta(days=1)
        for i in range(days_back):
            current_date = end_date - timedelta(days=days_back - 1 - i)
            date_list.append(current_date.strftime('%Y-%m-%d'))

        return date_list

    def sheets_refresh_token(self,authorization_code):
        token_url = "https://accounts.google.com/o/oauth2/token"
        payload = {
            "code": authorization_code,
            "client_id": self.sheets_client_id,
            "client_secret": self.sheets_client_secret,
            "redirect_uri": self.sheets_redirect_uri,
            "grant_type": "authorization_code",
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        response = requests.post(token_url, data=payload, headers=headers)
        if response.status_code == 200:
            token_data = response.json()
            refresh_token = token_data.get("refresh_token")
            if refresh_token:
                return refresh_token
            else:
                return "Ошибка: refresh_token не найден в ответе."
        else:
            error_data = response.json()
            error = error_data.get("error", "Неизвестная ошибка")
            error_description = error_data.get("error_description", "Нет описания ошибки")
            return f"Ошибка: {error} / {error_description}"

    def sheets_insert_data(self, refresh_token, spreadsheet_id, sheet_name="Sheet1",
                           data=[{'test': 'test_value', 'test2': 123}], clean=True):
        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=self.sheets_client_id,
            client_secret=self.sheets_client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        if not credentials.valid:
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
        gc = gspread.Client(auth=credentials)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        if clean:
            sheet.clear()

        # Формируем заголовки и строки данных
        headers = list(data[0].keys())
        headers = [item if item != '' else '-' for item in headers]
        rows = [headers]
        for item in data:
            row = []
            for value in item.values():
                # Заменяем пустые строки на пробел
                if value == None or str(value).strip()=='':
                    row.append("-")  # Заменяем на пробел
                else:
                    row.append(self.str_value(value))
            rows.append(row)

        table_text = str(sheet.acell('A1').value) + str(sheet.acell('A2').value)
        table_text = table_text.replace('None', '')
        print(table_text)

        if clean or table_text.strip() == '':
            sheet.update('A1', rows,value_input_option="USER_ENTERED")
        else:
            sheet.append_rows(rows[1:])  # append_rows не вставляет заголовки, поэтому начинаем с rows[1:]

        print(f"Данные успешно вставлены на лист '{sheet_name}'.")


    def sheets_delete_rows(self, refresh_token, spreadsheet_id, sheet_name="Sheet1", column_name='Column1',
                           value='test', contains=False):
        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=self.sheets_client_id,
            client_secret=self.sheets_client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        if not credentials.valid:
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
        gc = gspread.Client(auth=credentials)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Лист '{sheet_name}' не найден. Ничего не удалено.")
            return  # Выходим из функции, если лист не найден

        data = sheet.get_all_records()
        headers = sheet.row_values(1)
        try:
            column_index = headers.index(column_name) + 1
        except ValueError:
            print(f"Столбец '{column_name}' не найден в таблице. Ничего не удалено.")
            return  # Выходим из функции, если столбец не найден

        rows_to_delete = []
        for i, row in enumerate(data, start=2):
            cell_value = str(row[column_name])
            if contains:
                if str(value) in cell_value:
                    rows_to_delete.append(i)
            else:
                if cell_value == str(value):
                    rows_to_delete.append(i)

        # Оптимизация: удаляем строки группами
        if rows_to_delete:
            # Сортируем индексы строк в обратном порядке
            rows_to_delete.sort(reverse=True)

            # Формируем запросы на удаление строк
            requests = []
            for row_index in rows_to_delete:
                requests.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet.id,  # ID листа
                            "dimension": "ROWS",
                            "startIndex": row_index - 1,  # Индекс строки (начинается с 0)
                            "endIndex": row_index  # Конечный индекс (не включительно)
                        }
                    }
                })

            # Выполняем все запросы за один вызов API
            spreadsheet.batch_update({"requests": requests})

        print(
            f"Удалено {len(rows_to_delete)} строк на листе '{sheet_name}', где '{column_name}' {'содержит' if contains else 'равно'} '{value}'.")