from .common import Common
from .clickhouse import Clickhouse
from .wb_reklama import WBreklama
from .base_client import BaseMarketplaceClient
from datetime import datetime,timedelta
import clickhouse_connect
import pandas as pd
import os
from dateutil import parser
import time
import hashlib
from io import StringIO


class WBbyDate:
    def __init__(self, bot_token:str = '', chats:str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', token: str  = '',  start: str = '', backfill_days: int = 0, reports :str = ''):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.token = token
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.subd = subd
        self.add_name = self.common.transliterate_key(add_name)
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.yesterday = self.today - timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%Y-%m-%d")
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.platform = 'wb'
        self.err429 = False
        self.api = BaseMarketplaceClient(
            base_url='',
            headers={"Authorization": self.token},
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )
        self.common.log_func(self.bot_token, self.chat_list, f'Платформа: WB. Имя: {self.add_name}. HTTP-клиент: httpx', 1)
        self.source_dict = {
            'realized': {
                'platform': 'wb',
                'report_name': 'realized',
                'upload_table': 'realized',
                'func_name': self.get_realized,
                'uniq_columns': 'realizationreport_id,rrd_id',
                'partitions': 'realizationreport_id',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'Monday',
                'delay': 60
            },
            'orders': {
                'platform': 'wb',
                'report_name': 'orders',
                'upload_table': 'orders',
                'func_name': self.get_orders,
                'uniq_columns': 'date,srid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 60
            },
            'sbor_orders': {
                'platform': 'wb',
                'report_name': 'sbor_orders',
                'upload_table': 'sbor_orders',
                'func_name': self.get_sbor,
                'uniq_columns': 'id,rid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 10
            },
            'sbor_status': {
                'platform': 'wb',
                'report_name': 'sbor_status',
                'upload_table': 'sbor_status',
                'func_name': self.get_sbor_status,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 10
            },
            'incomes': {
                'platform': 'wb',
                'report_name': 'incomes',
                'upload_table': 'incomes',
                'func_name': self.get_incomes,
                'uniq_columns': 'incomeId,barcode',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'excise': {
                'platform': 'wb',
                'report_name': 'excise',
                'upload_table': 'excise',
                'func_name': self.get_excise,
                'uniq_columns': 'fiscal_dt,nm_id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'sales': {
                'platform': 'wb',
                'report_name': 'sales',
                'upload_table': 'sales',
                'func_name': self.get_sales,
                'uniq_columns': 'date,saleID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 60
            },
            'orders_changes': {
                'platform': 'wb',
                'report_name': 'orders_changes',
                'upload_table': 'orders',
                'func_name': self.get_orders_changes,
                'uniq_columns': 'date,srid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'sales_changes': {
                'platform': 'wb',
                'report_name': 'sales_changes',
                'upload_table': 'sales',
                'func_name': self.get_sales_changes,
                'uniq_columns': 'date,saleID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'stocks': {
                'platform': 'wb',
                'report_name': 'stocks',
                'upload_table': 'stocks',
                'func_name': self.get_stocks,
                'uniq_columns': 'lastChangeDate',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'cards': {
                'platform': 'wb',
                'report_name': 'cards',
                'upload_table': 'cards',
                'func_name': self.get_cards,
                'uniq_columns': 'nmID',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'stocks_history': {
                'platform': 'wb',
                'report_name': 'stocks_history',
                'upload_table': 'stocks_history',
                'func_name': self.get_stocks,
                'uniq_columns': 'lastChangeDate',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 60
            },
            'adv_upd': {
                'platform': 'wb',
                'report_name': 'adv_upd',
                'upload_table': 'adv_upd',
                'func_name': self.get_adv_upd,
                'uniq_columns': 'advertId,updTime,paymentType',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 5
            },
            'paid_storage': {
                'platform': 'wb',
                'report_name': 'paid_storage',
                'upload_table': 'paid_storage',
                'func_name': self.get_paid_storage,
                'uniq_columns': 'date',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',
                'delay': 60
            },
            'voronka_week': {
                'platform': 'wb',
                'report_name': 'voronka_week',
                'upload_table': 'voronka_week',
                'func_name': self.get_voronka_week,
                'uniq_columns': 'nmId,date',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 23
            },
            'voronka_all': {
                'platform': 'wb',
                'report_name': 'voronka_all',
                'upload_table': 'voronka_all',
                'func_name': self.get_voronka_all,
                'uniq_columns': 'product_nmId,statistic_selected_period_start',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 23
            },
            'feedbacks': {
                'platform': 'wb',
                'report_name': 'feedbacks',
                'upload_table': 'feedbacks',
                'func_name': self.get_feedbacks,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 10
            },
        }

    def _log_ok(self, func_name, date=''):
        message = f'Платформа: {self.platform.upper()}. Имя: {self.add_name}. Дата: {date}. Функция: {func_name}. Результат: ОК'
        self.common.log_func(self.bot_token, self.chat_list, message, 1)

    def _log_err(self, func_name, date='', error=''):
        if hasattr(self, 'api') and hasattr(self.api, 'err429') and self.api.err429:
            self.err429 = True
        message = f'Платформа: {self.platform.upper()}. Имя: {self.add_name}. Дата: {date}. Функция: {func_name}. Ошибка: {error}.'
        self.common.log_func(self.bot_token, self.chat_list, message, 3)
        return message

    def get_adv_upd(self, date):
        try:
            url = "https://advert-api.wildberries.ru/adv/v1/upd"
            params = {"from": date, "to": date}
            final_result = self.api._request('GET', url, params=params)
            self._log_ok('get_adv_upd', date)
            return final_result
        except Exception as e:
            return self._log_err('get_adv_upd', date, e)

    def get_sbor_status(self, date):
        try:
            url = "https://marketplace-api.wildberries.ru/api/v3/orders"
            next = '0'
            final_result = []
            while True:
                ids_to_collect = []
                params = {'limit': "1000", 'next': next,
                          "dateFrom": int(datetime.strptime( date+' 00:00:00', "%Y-%m-%d %H:%M:%S").timestamp()),
                          "dateTo" : int(datetime.strptime( date+' 23:59:59', "%Y-%m-%d %H:%M:%S").timestamp())}
                data = self.api._request('GET', url, params=params)
                next = str(data['next'])
                orders = data['orders']
                if len(orders)==0:
                    break
                for i in orders:
                    ids_to_collect.append(i['id'])
                status_url = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
                status_data = self.api._request('POST', status_url, json={"orders": ids_to_collect})
                final_result += status_data['orders']
                if len(orders)<1000:
                    break
                time.sleep(1)
            self._log_ok('get_sbor_status', date)
            return final_result
        except Exception as e:
            return self._log_err('get_sbor_status', date, e)


    def get_sbor(self, date):
        try:
            url = "https://marketplace-api.wildberries.ru/api/v3/orders"
            next = '0'
            final_result = []
            while True:
                params = {'limit': "1000", 'next': next,
                          "dateFrom": int(datetime.strptime( date+' 00:00:00', "%Y-%m-%d %H:%M:%S").timestamp()),
                          "dateTo" : int(datetime.strptime( date+' 23:59:59', "%Y-%m-%d %H:%M:%S").timestamp())}
                data = self.api._request('GET', url, params=params)
                orders = data['orders']
                if len(orders)==0:
                    break
                final_result += orders
                next = str(data['next'])
                if len(orders)<1000:
                    break
                time.sleep(1)
            self._log_ok('get_sbor', date)
            return final_result
        except Exception as e:
            return self._log_err('get_sbor', date, e)

    def create_ps_report(self, date1, date2):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
            params = {"dateFrom": date1, "dateTo": date2}
            data = self.api._request('GET', url, params=params)
            return data['data']['taskId']
        except Exception as e:
            return self._log_err('create_ps_report', f'{date1}-{date2}', e)


    def ps_report_status(self, task_id):
        try:
            url = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status"
            data = self.api._request('GET', url)
            return data['data']['status']
        except Exception as e:
            return self._log_err('ps_report_status', '', e)


    def get_ps_report(self, task_id):
        try:
            url = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download"
            data = self.api._request('GET', url)
            return data
        except Exception as e:
            return self._log_err('get_ps_report', '', e)


    def get_paid_storage(self, date):
        try:
            task = self.create_ps_report(date, date)
            for t in range(20):
                time.sleep(10)
                if self.ps_report_status(task) =='done':
                    self._log_ok('get_paid_storage', date)
                    return self.get_ps_report(task)
        except Exception as e:
            return self._log_err('get_paid_storage', date, e)


    def get_orders(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            params = {"dateFrom": date_rfc3339, "flag": 1}
            final_result = self.api._request('GET', url, params=params)
            self._log_ok('get_orders', date)
            return final_result
        except Exception as e:
            return self._log_err('get_orders', date, e)

    def get_incomes(self, date=''):
        try:
            date_rfc3339 = f"{self.start}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"
            params = {"dateFrom": date_rfc3339}
            json_data = self.api._request('GET', url, params=params, timeout=200)
            if not json_data or all(not item for item in json_data if isinstance(json_data, list)):
                raise ValueError("Получен пустой Json")
            final_result = json_data
            self._log_ok('get_incomes', date)
            return final_result
        except Exception as e:
            return self._log_err('get_incomes', date, e)

    def get_orders_changes(self, date):
        try:
            date_rfc3339 = f"{date}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            params = {"dateFrom": date_rfc3339}
            final_result = self.api._request('GET', url, params=params)
            self._log_ok('get_orders_changes', date)
            return final_result
        except Exception as e:
            return self._log_err('get_orders_changes', date, e)

    def get_sales(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            params = {'dateFrom': date, "flag": 1}
            final_result = self.api._request('GET', url, params=params, headers={'Authorization': f'Bearer {self.token}'})
            self._log_ok('get_sales', date)
            return final_result
        except Exception as e:
            return self._log_err('get_sales', date, e)

    def get_excise(self, date):
        try:
            url = 'https://seller-analytics-api.wildberries.ru/api/v1/analytics/excise-report'
            params = {'dateFrom': self.start, 'dateTo': self.yesterday_str}
            data = self.api._request('POST', url, params=params, json={})
            final_result = data['response']['data']
            self._log_ok('get_excise', date)
            return final_result
        except Exception as e:
            return self._log_err('get_excise', date, e)


    def get_sales_changes(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
            params = {'dateFrom': date}
            final_result = self.api._request('GET', url, params=params, headers={'Authorization': f'Bearer {self.token}'})
            self._log_ok('get_sales_changes', date)
            return final_result
        except Exception as e:
            return self._log_err('get_sales_changes', date, e)

    def get_realized(self, date):
        try:
            url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod'
            params = {'dateFrom': self.common.shift_date(date,7), 'dateTo': self.common.shift_date(date,1)}
            final_result = self.api._request('GET', url, params=params, headers={'Authorization': f'Bearer {self.token}'})
            self._log_ok('get_realized', date)
            return final_result
        except Exception as e:
            return self._log_err('get_realized', date, e)

    def get_stocks(self, date=''):
        try:
            date_rfc3339 = f"{self.start}T00:00:00.000Z"
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
            params = {"dateFrom": date_rfc3339}
            final_result = self.api._request('GET', url, params=params)
            self._log_ok('get_stocks', date)
            return final_result
        except Exception as e:
            return self._log_err('get_stocks', date, e)

    def get_voronka_week(self, date):
        try:
            self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                                         self.username, self.password,
                                         self.database, self.start, self.add_name, self.err429, self.backfill_days,
                                         self.platform)
            nm_list_raw = self.clickhouse.get_table_data(f'{self.platform}_cards_{self.add_name}', ' nmID ')
            nm_list = [row['nmID'] for row in nm_list_raw] if nm_list_raw else []
            final_list = self.common.get_chunks(nm_list,20)
            url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products/history"
            all_cards = []
            for chunk in final_list:
                payload = {
                        "selectedPeriod": {
                            "start": f"{date}",
                            "end": f"{date}"
                        },
                    "nmIds": chunk,
                        "skipDeletedNm": True,
                        "aggregationLevel": "day"
                    }
                data = self.api._request('POST', url, json=payload)
                for card in data:
                    if len(card['history']) == 1:
                        card_dict = card['product'] | card['history'][0]
                        all_cards.append(card_dict)
                time.sleep(23)
            self._log_ok('get_voronka_week', date)
            return self.common.spread_table(self.common.spread_table(self.common.spread_table(all_cards)))
        except Exception as e:
            return self._log_err('get_voronka_week', date, e)

    def get_voronka_all(self, date):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
            offset = 0
            limit = 1000
            all_cards = []
            while True:
                payload = {
                        "selectedPeriod": {
                            "start": f"{date}",
                            "end": f"{date}"
                        },
                        "skipDeletedNm": True,
                    "limit" : limit,
                    "offset" : offset
                    }
                data = self.api._request('POST', url, json=payload)
                products = data['data']['products']
                all_cards.extend(products)
                if len(products)<limit:
                    break
                offset += limit
                time.sleep(23)
            self._log_ok('get_voronka_all', date)
            return self.common.spread_table(self.common.spread_table(self.common.spread_table(all_cards)))
        except Exception as e:
            return self._log_err('get_voronka_all', date, e)


    def get_cards(self, date=''):
        try:
            url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
            all_cards = []
            cursor = {"limit": 100}

            while True:
                payload = {
                    "settings": {
                        "cursor": cursor,
                        "filter": {
                            "withPhoto": -1
                        }
                    }
                }
                data = self.api._request('POST', url, json=payload)
                cards = data.get('cards', [])
                cursor_info = data.get('cursor', {})

                if not cards:
                    break

                all_cards.extend(cards)

                total = cursor_info.get('total', 0)
                if total < 100:
                    break

                if 'updatedAt' in cursor_info and 'nmID' in cursor_info:
                    cursor = {
                        "limit": 100,
                        "updatedAt": cursor_info['updatedAt'],
                        "nmID": cursor_info['nmID']
                    }
                else:
                    break

                time.sleep(2)

            self._log_ok('get_cards', date)
            return self.common.spread_table(self.common.spread_table(all_cards))

        except Exception as e:
            return self._log_err('get_cards', date, e)


    def get_chosen_feedbacks(self, date, answered):
        try:
            take = 5000
            url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
            all_feedbacks = []
            skip = 0
            while True:
                params = {'order': 'dateAsc', 'isAnswered': answered, 'take': str(take), 'skip': str(skip), "dateFrom": str(self.common.datetime_to_unixtime(date +' 00:00:00')), "dateTo": str(self.common.datetime_to_unixtime(date+ ' 23:59:59'))}
                data = self.api._request('GET', url, params=params)
                result = data['data']['feedbacks']
                if len(result) == 0:
                    break
                all_feedbacks.extend(result)
                skip = skip+ take
                time.sleep(2)
            return all_feedbacks
        except Exception as e:
            return self._log_err('get_chosen_feedbacks', date, e)


    def get_feedbacks(self, date):
        try:
            all_feedbacks = []
            all_feedbacks.extend(self.get_chosen_feedbacks(date, "true"))
            all_feedbacks.extend(self.get_chosen_feedbacks(date, "false"))
            self._log_ok('get_feedbacks', date)
            return self.common.spread_table(self.common.spread_table(all_feedbacks))
        except Exception as e:
            return self._log_err('get_feedbacks', date, e)

    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            if report == 'reklama':
                self.reklama = WBreklama(self.bot_token, self.chat_list, self.message_type, self.subd, self.add_name, self.token, self.host, self.port, self.username, self.password,
                                             self.database, self.start,  self.backfill_days,)
                self.reklama.wb_reklama_collector()
            else:
                self.clickhouse = Clickhouse(self.bot_token, self.chat_list, self.message_type, self.host, self.port, self.username, self.password,
                                             self.database, self.start, self.add_name, self.err429, self.backfill_days, self.platform)
                self.clickhouse.collecting_report(
                    self.source_dict[report]['platform'],
                    self.source_dict[report]['report_name'],
                    self.source_dict[report]['upload_table'],
                    self.source_dict[report]['func_name'],
                    self.source_dict[report]['uniq_columns'],
                    self.source_dict[report]['partitions'],
                    self.source_dict[report]['merge_type'],
                    self.source_dict[report]['refresh_type'],
                    self.source_dict[report]['history'],
                    self.source_dict[report]['frequency'],
                    self.source_dict[report]['delay']
                )
        self.common.send_logs_clear_anyway(self.bot_token, self.chat_list)
