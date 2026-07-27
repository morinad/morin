from .common import Common
from .clickhouse import Clickhouse
from .db import make_db
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
import ast
import json


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
                'delay': 65
            },
            'realized_list': {
                'platform': 'wb',
                'report_name': 'realized_list',
                'upload_table': 'realized_list',
                'func_name': self.get_realized_list,
                'uniq_columns': 'reportId',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'Monday',
                'delay': 65
            },
            'realized_detail': {
                'platform': 'wb',
                'report_name': 'realized_detail',
                'upload_table': 'realized_detail',
                'func_name': self.get_realized_detail,
                'uniq_columns': 'reportId,rrdId',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'Monday',
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
                'delay': 65
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
            'warehouses': {
                'platform': 'wb',
                'report_name': 'warehouses',
                'upload_table': 'warehouses',
                'func_name': self.get_warehouses,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 5
            },
            'stocks_wh': {
                'platform': 'wb',
                'report_name': 'stocks_wh',
                'upload_table': 'stocks_wh',
                'func_name': self.get_stocks_wh,
                'uniq_columns': 'warehouse_id,chrtId',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 10
            },
            'stocks_wh_history': {
                'platform': 'wb',
                'report_name': 'stocks_wh_history',
                'upload_table': 'stocks_wh_history',
                'func_name': self.get_stocks_wh,
                'uniq_columns': 'warehouse_id,chrtId',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 10
            },
            'warehouse_remains': {
                'platform': 'wb',
                'report_name': 'warehouse_remains',
                'upload_table': 'warehouse_remains',
                'func_name': self.get_warehouse_remains,
                'uniq_columns': 'nmId,barcode,techSize,warehouseName',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 65
            },
            'warehouse_remains_history': {
                'platform': 'wb',
                'report_name': 'warehouse_remains_history',
                'upload_table': 'warehouse_remains_history',
                'func_name': self.get_warehouse_remains,
                'uniq_columns': 'nmId,barcode,techSize,warehouseName',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 65
            },
            'offices': {
                'platform': 'wb',
                'report_name': 'offices',
                'upload_table': 'offices',
                'func_name': self.get_offices,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 5
            },
            'tariffs_commission': {
                'platform': 'wb',
                'report_name': 'tariffs_commission',
                'upload_table': 'tariffs_commission',
                'func_name': self.get_tariffs_commission,
                'uniq_columns': 'subjectID,parentID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 65
            },
            'discounts_prices': {
                'platform': 'wb',
                'report_name': 'discounts_prices',
                'upload_table': 'discounts_prices',
                'func_name': self.get_discounts_prices,
                'uniq_columns': 'nmID,sizeID',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 10
            },
            'tariffs_box': {
                'platform': 'wb',
                'report_name': 'tariffs_box',
                'upload_table': 'tariffs_box',
                'func_name': self.get_tariffs_box,
                'uniq_columns': 'date,warehouseName',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 65
            },
            'tariffs_pallet': {
                'platform': 'wb',
                'report_name': 'tariffs_pallet',
                'upload_table': 'tariffs_pallet',
                'func_name': self.get_tariffs_pallet,
                'uniq_columns': 'date,warehouseName',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 65
            },
            'goods_return': {
                'platform': 'wb',
                'report_name': 'goods_return',
                'upload_table': 'goods_return',
                'func_name': self.get_goods_return,
                'uniq_columns': 'date,barcode,nmID,srid',
                'partitions': 'date',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 65
            },
            'tariffs_return': {
                'platform': 'wb',
                'report_name': 'tariffs_return',
                'upload_table': 'tariffs_return',
                'func_name': self.get_tariffs_return,
                'uniq_columns': 'date,warehouseName',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 5
            },
            'acceptance_coefficients': {
                'platform': 'wb',
                'report_name': 'acceptance_coefficients',
                'upload_table': 'acceptance_coefficients',
                'func_name': self.get_acceptance_coefficients,
                'uniq_columns': 'date,warehouseID,boxTypeID',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 15
            },
            'calendar_promotions': {
                'platform': 'wb',
                'report_name': 'calendar_promotions',
                'upload_table': 'calendar_promotions',
                'func_name': self.get_calendar_promotions,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 10
            },
            'documents_list': {
                'platform': 'wb',
                'report_name': 'documents_list',
                'upload_table': 'documents_list',
                'func_name': self.get_documents_list,
                'uniq_columns': 'serviceName,groupName,serviceType',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 10
            },
            'item_rating': {
                'platform': 'wb',
                'report_name': 'item_rating',
                'upload_table': 'item_rating',
                'func_name': self.get_item_rating,
                'uniq_columns': 'nmID',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 30
            },
            'region_sale': {
                'platform': 'wb',
                'report_name': 'region_sale',
                'upload_table': 'region_sale',
                'func_name': self.get_region_sale,
                'uniq_columns': 'dateFrom,regionName,countryName,saleWithDiscount10',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 15
            },
            'acquiring_list': {
                'platform': 'wb',
                'report_name': 'acquiring_list',
                'upload_table': 'acquiring_list',
                'func_name': self.get_acquiring_list,
                'uniq_columns': 'reportId',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'Monday',
                'delay': 65
            },
            'acquiring_detail': {
                'platform': 'wb',
                'report_name': 'acquiring_detail',
                'upload_table': 'acquiring_detail',
                'func_name': self.get_acquiring_detail,
                'uniq_columns': 'reportId,rrdId',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'Monday',
                'delay': 65
            },
            'supplies_fbw': {
                'platform': 'wb',
                'report_name': 'supplies_fbw',
                'upload_table': 'supplies_fbw',
                'func_name': self.get_supplies_fbw,
                'uniq_columns': 'supplyId,barcode,nmId',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'deductions': {
                'platform': 'wb',
                'report_name': 'deductions',
                'upload_table': 'deductions',
                'func_name': self.get_deductions,
                'uniq_columns': 'type,dateFrom,uid',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 15
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
            response = self.api._request_raw('GET', url)
            if not response.content:
                return []
            return response.json()
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

    def create_warehouse_remains_report(self):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
            params = {
                "groupByBrand": "true",
                "groupBySubject": "true",
                "groupBySa": "true",
                "groupByNm": "true",
                "groupByBarcode": "true",
                "groupBySize": "true",
            }
            data = self.api._request('GET', url, params=params)
            return data.get('data', {}).get('taskId')
        except Exception as e:
            return self._log_err('create_warehouse_remains_report', '', e)

    def warehouse_remains_status(self, task_id):
        try:
            url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/status"
            data = self.api._request('GET', url)
            return data.get('data', {}).get('status')
        except Exception as e:
            return self._log_err('warehouse_remains_status', '', e)

    def get_warehouse_remains_download(self, task_id):
        try:
            url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
            response = self.api._request_raw('GET', url)
            if not response.content:
                return []
            return response.json()
        except Exception as e:
            return self._log_err('get_warehouse_remains_download', '', e)

    def get_warehouse_remains(self, date=''):
        try:
            task = self.create_warehouse_remains_report()
            if not task or (isinstance(task, str) and 'Ошибка' in task):
                return []
            for _ in range(60):
                time.sleep(10)
                status = self.warehouse_remains_status(task)
                if status == 'done':
                    break
            raw = self.get_warehouse_remains_download(task)
            if not raw or not isinstance(raw, list):
                return []
            result = []
            for item in raw:
                wh_list = item.get('warehouses') or []
                base = {k: v for k, v in item.items() if k != 'warehouses'}
                if not wh_list:
                    result.append(base)
                    continue
                for wh in wh_list:
                    row = dict(base)
                    for k, v in wh.items():
                        row[k] = v
                    result.append(row)
            self._log_ok('get_warehouse_remains', date)
            return self.common.spread_table(self.common.spread_table(result))
        except Exception as e:
            return self._log_err('get_warehouse_remains', date, e)


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

    def get_realized_list(self, date):
        try:
            url = 'https://finance-api.wildberries.ru/api/finance/v1/sales-reports/list'
            date_from = self.common.shift_date(date, 7)
            date_to = self.common.shift_date(date, 1)
            all_rows = []
            offset = 0
            limit = 1000
            for _ in range(100):
                payload = {
                    'dateFrom': date_from,
                    'dateTo': date_to,
                    'period': 'weekly',
                    'limit': limit,
                    'offset': offset
                }
                data = self.api._request('POST', url, json=payload, headers={'Authorization': f'Bearer {self.token}'})
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = data.get('data') or data.get('list') or data.get('reports') or []
                else:
                    rows = []
                if not rows:
                    break
                all_rows.extend(rows)
                if len(rows) < limit:
                    break
                offset += limit
                time.sleep(65)
            self._log_ok('get_realized_list', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_realized_list', date, e)

    def get_realized_detail(self, date):
        try:
            url = 'https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed'
            date_from = self.common.shift_date(date, 7)
            date_to = self.common.shift_date(date, 1)
            all_rows = []
            rrd_id = 0
            for _ in range(200):
                payload = {
                    'dateFrom': date_from,
                    'dateTo': date_to,
                    'period': 'weekly',
                    'limit': 100000,
                    'rrdId': rrd_id
                }
                response = self.api._request_raw('POST', url, json=payload, headers={'Authorization': f'Bearer {self.token}'})
                if response.status_code == 204:
                    break
                try:
                    data = response.json()
                except Exception:
                    break
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = data.get('data') or []
                else:
                    rows = []
                if not rows:
                    break
                all_rows.extend(rows)
                last_row = rows[-1] if isinstance(rows[-1], dict) else {}
                last_rrd = last_row.get('rrdId') or last_row.get('rrd_id')
                if not last_rrd or last_rrd == rrd_id:
                    break
                rrd_id = last_rrd
                time.sleep(65)
            self._log_ok('get_realized_detail', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_realized_detail', date, e)

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
            self.clickhouse = make_db(self.subd, self.bot_token, self.chat_list, self.message_type, self.host, self.port,
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
            limit = 100
            all_cards = []
            cursor = {"limit": limit}

            for _ in range(1000):
                payload = {
                    "settings": {
                        "sort": {"ascending": True},
                        "filter": {"withPhoto": -1},
                        "cursor": cursor
                    }
                }
                data = self.api._request('POST', url, json=payload)
                cards = data.get('cards', []) or []
                cursor_info = data.get('cursor', {}) or {}
                if cards:
                    all_cards.extend(cards)
                total = cursor_info.get('total', 0)
                if total < limit:
                    break
                next_updated = cursor_info.get('updatedAt')
                next_nm = cursor_info.get('nmID')
                if not next_updated or not next_nm:
                    break
                cursor = {
                    "limit": limit,
                    "updatedAt": next_updated,
                    "nmID": next_nm
                }
                time.sleep(1)

            self._log_ok('get_cards', date)
            return self.common.spread_table(self.common.spread_table(all_cards))

        except Exception as e:
            return self._log_err('get_cards', date, e)


    def get_warehouses(self, date=''):
        try:
            url = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
            data = self.api._request('GET', url)
            if isinstance(data, list):
                result = data
            elif isinstance(data, dict):
                result = data.get('data') or data.get('warehouses') or []
            else:
                result = []
            self._log_ok('get_warehouses', date)
            return result
        except Exception as e:
            return self._log_err('get_warehouses', date, e)

    def get_offices(self, date=''):
        try:
            url = "https://marketplace-api.wildberries.ru/api/v3/offices"
            data = self.api._request('GET', url)
            if isinstance(data, list):
                result = data
            elif isinstance(data, dict):
                result = data.get('data') or data.get('offices') or []
            else:
                result = []
            self._log_ok('get_offices', date)
            return result
        except Exception as e:
            return self._log_err('get_offices', date, e)

    def get_tariffs_commission(self, date=''):
        try:
            url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
            data = self.api._request('GET', url, params={'locale': 'ru'})
            if isinstance(data, dict):
                result = data.get('report') or data.get('data', {}).get('report') or []
            elif isinstance(data, list):
                result = data
            else:
                result = []
            self._log_ok('get_tariffs_commission', date)
            return result
        except Exception as e:
            return self._log_err('get_tariffs_commission', date, e)

    def get_discounts_prices(self, date=''):
        try:
            url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
            limit = 1000
            offset = 0
            all_items = []
            for _ in range(500):
                data = self.api._request('GET', url, params={'limit': limit, 'offset': offset})
                items = (data.get('data') or {}).get('listGoods') or []
                if not items:
                    break
                for item in items:
                    base = {k: v for k, v in item.items() if k != 'sizes'}
                    sizes = item.get('sizes') or []
                    if not sizes:
                        all_items.append(base)
                        continue
                    for sz in sizes:
                        row = dict(base)
                        for k, v in sz.items():
                            row[k] = v
                        all_items.append(row)
                if len(items) < limit:
                    break
                offset += limit
                time.sleep(1)
            self._log_ok('get_discounts_prices', date)
            return self.common.spread_table(self.common.spread_table(all_items))
        except Exception as e:
            return self._log_err('get_discounts_prices', date, e)

    def _get_tariffs(self, url, endpoint_name, list_key, date):
        try:
            data = self.api._request('GET', url, params={'date': date})
            data_block = ((data.get('response') or {}).get('data')) or data.get('data') or {}
            items = data_block.get(list_key) or data_block.get('warehouseList') or []
            currency = data_block.get('currency', '')
            dt_from = data_block.get('dtNextBox') or data_block.get('dtNextPallet') or ''
            dt_till = data_block.get('dtTillMax') or ''
            for item in items:
                item['date'] = date
                if currency:
                    item['currency'] = currency
                if dt_from:
                    item['dtNext'] = dt_from
                if dt_till:
                    item['dtTillMax'] = dt_till
            self._log_ok(endpoint_name, date)
            return self.common.spread_table(items)
        except Exception as e:
            return self._log_err(endpoint_name, date, e)

    def get_tariffs_box(self, date):
        return self._get_tariffs('https://common-api.wildberries.ru/api/v1/tariffs/box', 'get_tariffs_box', 'warehouseList', date)

    def get_tariffs_pallet(self, date):
        return self._get_tariffs('https://common-api.wildberries.ru/api/v1/tariffs/pallet', 'get_tariffs_pallet', 'warehouseList', date)

    def get_goods_return(self, date):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/goods-return"
            data = self.api._request('GET', url, params={'dateFrom': date, 'dateTo': date})
            report = data.get('report') or (data.get('data') or {}).get('report') or []
            for r in report:
                if 'date' not in r:
                    r['date'] = date
            self._log_ok('get_goods_return', date)
            return self.common.spread_table(report)
        except Exception as e:
            return self._log_err('get_goods_return', date, e)

    def get_tariffs_return(self, date):
        return self._get_tariffs('https://common-api.wildberries.ru/api/v1/tariffs/return', 'get_tariffs_return', 'warehouseList', date)

    def get_acceptance_coefficients(self, date=''):
        try:
            url = "https://supplies-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients"
            data = self.api._request('GET', url)
            if isinstance(data, list):
                result = data
            elif isinstance(data, dict):
                result = data.get('data') or data.get('result') or []
            else:
                result = []
            self._log_ok('get_acceptance_coefficients', date)
            return self.common.spread_table(result)
        except Exception as e:
            return self._log_err('get_acceptance_coefficients', date, e)

    def get_calendar_promotions(self, date=''):
        try:
            url = "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions"
            start_dt = self.today.strftime('%Y-%m-%dT00:00:00Z')
            end_dt = (self.today + timedelta(days=90)).strftime('%Y-%m-%dT23:59:59Z')
            limit = 1000
            offset = 0
            all_items = []
            for _ in range(100):
                params = {'startDateTime': start_dt, 'endDateTime': end_dt, 'allPromo': 'true',
                          'limit': limit, 'offset': offset}
                data = self.api._request('GET', url, params=params)
                items = (data.get('data') or {}).get('promotions') or []
                if not items:
                    break
                all_items.extend(items)
                if len(items) < limit:
                    break
                offset += limit
                time.sleep(1)
            self._log_ok('get_calendar_promotions', date)
            return self.common.spread_table(self.common.spread_table(all_items))
        except Exception as e:
            return self._log_err('get_calendar_promotions', date, e)

    def get_documents_list(self, date=''):
        try:
            url = "https://documents-api.wildberries.ru/api/v1/documents/list"
            data = self.api._request('GET', url)
            data_block = data.get('data') if isinstance(data, dict) else None
            if isinstance(data_block, list):
                result = data_block
            elif isinstance(data_block, dict):
                result = data_block.get('documents') or data_block.get('list') or []
            elif isinstance(data, list):
                result = data
            else:
                result = []
            self._log_ok('get_documents_list', date)
            return self.common.spread_table(self.common.spread_table(result))
        except Exception as e:
            return self._log_err('get_documents_list', date, e)

    def get_item_rating(self, date=''):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/analytics/v2/item-rating"
            end_date = (self.today - timedelta(days=1)).strftime('%Y-%m-%d')
            start_date = (self.today - timedelta(days=30)).strftime('%Y-%m-%d')
            past_end = (self.today - timedelta(days=31)).strftime('%Y-%m-%d')
            past_start = (self.today - timedelta(days=60)).strftime('%Y-%m-%d')
            payload = {
                'currentPeriod': {'start': start_date, 'end': end_date},
                'pastPeriod': {'start': past_start, 'end': past_end},
            }
            data = self.api._request('POST', url, json=payload)
            data_block = data.get('data') if isinstance(data, dict) else None
            items = []
            if isinstance(data_block, dict):
                items = data_block.get('items') or data_block.get('list') or []
            elif isinstance(data_block, list):
                items = data_block
            elif isinstance(data, list):
                items = data
            self._log_ok('get_item_rating', date)
            return self.common.spread_table(self.common.spread_table(items))
        except Exception as e:
            return self._log_err('get_item_rating', date, e)

    def get_region_sale(self, date):
        try:
            url = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/region-sale"
            data = self.api._request('GET', url, params={'dateFrom': date, 'dateTo': date})
            data_block = data.get('data') if isinstance(data, dict) else None
            report = []
            if isinstance(data_block, list):
                report = data_block
            elif isinstance(data_block, dict):
                report = data_block.get('regions') or data_block.get('report') or data_block.get('list') or []
            elif isinstance(data, list):
                report = data
            elif isinstance(data, dict):
                report = data.get('report') or []
            for r in report:
                if isinstance(r, dict) and 'dateFrom' not in r:
                    r['dateFrom'] = date
            self._log_ok('get_region_sale', date)
            return self.common.spread_table(self.common.spread_table(report))
        except Exception as e:
            return self._log_err('get_region_sale', date, e)

    def get_acquiring_list(self, date):
        try:
            url = 'https://finance-api.wildberries.ru/api/finance/v1/acquiring/list'
            date_from = self.common.shift_date(date, 7)
            date_to = self.common.shift_date(date, 1)
            all_rows = []
            offset = 0
            limit = 1000
            for _ in range(100):
                payload = {'dateFrom': date_from, 'dateTo': date_to, 'limit': limit, 'offset': offset}
                data = self.api._request('POST', url, json=payload, headers={'Authorization': f'Bearer {self.token}'})
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = data.get('data') or data.get('list') or data.get('reports') or []
                else:
                    rows = []
                if not rows:
                    break
                all_rows.extend(rows)
                if len(rows) < limit:
                    break
                offset += limit
                time.sleep(65)
            self._log_ok('get_acquiring_list', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_acquiring_list', date, e)

    def get_supplies_fbw(self, date=''):
        try:
            url_list = "https://supplies-api.wildberries.ru/api/v1/supplies"
            all_supplies = []
            offset = 0
            limit = 1000
            for _ in range(50):
                data = self.api._request('POST', url_list, params={'limit': limit, 'offset': offset}, json={})
                supplies = data.get('supplies') or (data.get('data') or {}).get('supplies') or []
                if not supplies:
                    break
                all_supplies.extend(supplies)
                if len(supplies) < limit:
                    break
                offset += limit
                time.sleep(2)

            all_rows = []
            for s in all_supplies:
                sid = s.get('supplyId') or s.get('id')
                if not sid:
                    all_rows.append(s)
                    continue
                try:
                    goods_data = self.api._request('GET', f'https://supplies-api.wildberries.ru/api/v1/supplies/{sid}/goods')
                    goods = goods_data.get('goods') or (goods_data.get('data') or {}).get('goods') or []
                    base = {k: v for k, v in s.items() if k not in ('goods',)}
                    base['supplyId'] = sid
                    if not goods:
                        all_rows.append(base)
                    else:
                        for g in goods:
                            row = dict(base)
                            for k, v in g.items():
                                row[k] = v
                            all_rows.append(row)
                except Exception:
                    all_rows.append(s)
                time.sleep(2)

            self._log_ok('get_supplies_fbw', date)
            return self.common.spread_table(self.common.spread_table(all_rows))
        except Exception as e:
            return self._log_err('get_supplies_fbw', date, e)

    def get_deductions(self, date):
        try:
            endpoints = [
                ('measurement_penalties', 'https://seller-analytics-api.wildberries.ru/api/analytics/v1/measurement-penalties'),
                ('warehouse_measurements', 'https://seller-analytics-api.wildberries.ru/api/analytics/v1/warehouse-measurements'),
                ('deductions', 'https://seller-analytics-api.wildberries.ru/api/analytics/v1/deductions'),
                ('antifraud_details', 'https://seller-analytics-api.wildberries.ru/api/v1/analytics/antifraud-details'),
                ('goods_labeling', 'https://seller-analytics-api.wildberries.ru/api/v1/analytics/goods-labeling'),
            ]
            all_rows = []
            for type_name, url in endpoints:
                try:
                    data = self.api._request('GET', url, params={'dateFrom': date, 'dateTo': date})
                    data_block = data.get('data') if isinstance(data, dict) else None
                    report = []
                    if isinstance(data_block, list):
                        report = data_block
                    elif isinstance(data_block, dict):
                        report = data_block.get('report') or data_block.get('details') or data_block.get('items') or []
                    elif isinstance(data, dict):
                        report = data.get('report') or []
                    elif isinstance(data, list):
                        report = data
                    for r in report:
                        if isinstance(r, dict):
                            r['type'] = type_name
                            if 'dateFrom' not in r:
                                r['dateFrom'] = date
                            if 'uid' not in r:
                                r['uid'] = str(r.get('id') or r.get('srid') or r.get('nmID') or '') + '_' + str(r.get('createdAt') or r.get('date') or '')
                            all_rows.append(r)
                    time.sleep(2)
                except Exception as e:
                    message = f'Платформа: WB. Имя: {self.add_name}. Дата: {date}. Функция: get_deductions/{type_name}. Ошибка: {e}.'
                    self.common.log_func(self.bot_token, self.chat_list, message, 3)
            self._log_ok('get_deductions', date)
            return self.common.spread_table(self.common.spread_table(all_rows))
        except Exception as e:
            return self._log_err('get_deductions', date, e)

    def get_acquiring_detail(self, date):
        try:
            url = 'https://finance-api.wildberries.ru/api/finance/v1/acquiring/detailed'
            date_from = self.common.shift_date(date, 7)
            date_to = self.common.shift_date(date, 1)
            all_rows = []
            rrd_id = 0
            for _ in range(200):
                payload = {'dateFrom': date_from, 'dateTo': date_to, 'limit': 100000, 'rrdId': rrd_id}
                response = self.api._request_raw('POST', url, json=payload, headers={'Authorization': f'Bearer {self.token}'})
                if response.status_code == 204:
                    break
                try:
                    data = response.json()
                except Exception:
                    break
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = data.get('data') or []
                else:
                    rows = []
                if not rows:
                    break
                all_rows.extend(rows)
                last_row = rows[-1] if isinstance(rows[-1], dict) else {}
                last_rrd = last_row.get('rrdId') or last_row.get('rrd_id')
                if not last_rrd or last_rrd == rrd_id:
                    break
                rrd_id = last_rrd
                time.sleep(65)
            self._log_ok('get_acquiring_detail', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_acquiring_detail', date, e)

    def _get_all_chrt_ids(self):
        try:
            url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
            limit = 100
            cursor = {"limit": limit}
            chrt_ids = set()
            for _ in range(1000):
                payload = {
                    "settings": {
                        "sort": {"ascending": True},
                        "filter": {"withPhoto": -1},
                        "cursor": cursor
                    }
                }
                data = self.api._request('POST', url, json=payload)
                cards = data.get('cards', []) or []
                cursor_info = data.get('cursor', {}) or {}
                for card in cards:
                    for size in card.get('sizes', []) or []:
                        chrt_id = size.get('chrtID')
                        if chrt_id:
                            chrt_ids.add(chrt_id)
                total = cursor_info.get('total', 0)
                if total < limit:
                    break
                next_updated = cursor_info.get('updatedAt')
                next_nm = cursor_info.get('nmID')
                if not next_updated or not next_nm:
                    break
                cursor = {"limit": limit, "updatedAt": next_updated, "nmID": next_nm}
                time.sleep(1)
            return list(chrt_ids)
        except Exception as e:
            self._log_err('_get_all_chrt_ids', '', e)
            return []

    def _get_warehouses_from_db(self):
        try:
            db = make_db(self.subd, self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                         self.username, self.password, self.database, self.start, self.add_name, self.err429,
                         self.backfill_days, self.platform)
            rows = db.get_table_data(f'wb_warehouses_{self.add_name}', ['id', 'officeId', 'name'])
            if not rows:
                return []
            result = []
            for row in rows:
                wh_id = row.get('id')
                if wh_id is None:
                    continue
                result.append({
                    'id': int(wh_id) if wh_id is not None else None,
                    'officeId': row.get('officeId'),
                    'name': row.get('name', '') or ''
                })
            return result
        except Exception:
            return []

    def _get_chrt_ids_from_db(self):
        try:
            db = make_db(self.subd, self.bot_token, self.chat_list, self.message_type, self.host, self.port,
                         self.username, self.password, self.database, self.start, self.add_name, self.err429,
                         self.backfill_days, self.platform)
            rows = db.get_table_data(f'wb_cards_{self.add_name}', ['sizes'])
            if not rows:
                return []
            chrt_ids = set()
            for row in rows:
                sizes_raw = row.get('sizes')
                if not sizes_raw:
                    continue
                sizes_list = None
                if isinstance(sizes_raw, list):
                    sizes_list = sizes_raw
                elif isinstance(sizes_raw, str):
                    try:
                        sizes_list = json.loads(sizes_raw)
                    except Exception:
                        try:
                            sizes_list = ast.literal_eval(sizes_raw)
                        except Exception:
                            sizes_list = None
                if not sizes_list or not isinstance(sizes_list, list):
                    continue
                for size in sizes_list:
                    if not isinstance(size, dict):
                        continue
                    chrt = size.get('chrtID') or size.get('chrtId')
                    if chrt:
                        try:
                            chrt_ids.add(int(chrt))
                        except Exception:
                            pass
            return list(chrt_ids)
        except Exception:
            return []

    def get_stocks_wh(self, date=''):
        try:
            warehouses = self._get_warehouses_from_db()
            src_wh = 'БД'
            if not warehouses:
                warehouses = self.get_warehouses()
                src_wh = 'API'
            if not warehouses or not isinstance(warehouses, list):
                self._log_ok('get_stocks_wh (нет складов)', date)
                return []

            chrt_ids = self._get_chrt_ids_from_db()
            src_chrt = 'БД'
            if not chrt_ids:
                chrt_ids = self._get_all_chrt_ids()
                src_chrt = 'API'
            if not chrt_ids:
                self._log_ok('get_stocks_wh (нет chrtIds)', date)
                return []

            message = f'Платформа: WB. Имя: {self.add_name}. Функция: get_stocks_wh. Складов: {len(warehouses)} ({src_wh}), chrtIds: {len(chrt_ids)} ({src_chrt}).'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)

            all_stocks = []
            for wh in warehouses:
                wh_id = wh.get('id')
                if not wh_id:
                    continue
                wh_name = wh.get('name', '') or ''
                wh_office_id = wh.get('officeId')
                url = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{wh_id}"
                for i in range(0, len(chrt_ids), 1000):
                    batch = chrt_ids[i:i + 1000]
                    try:
                        data = self.api._request('POST', url, json={"chrtIds": batch})
                    except Exception as e:
                        message = f'Платформа: WB. Имя: {self.add_name}. Функция: get_stocks_wh. Склад: {wh_id}. Батч: {i}. Ошибка: {e}.'
                        self.common.log_func(self.bot_token, self.chat_list, message, 3)
                        continue
                    stocks = data.get('stocks', []) or []
                    for s in stocks:
                        all_stocks.append({
                            'warehouse_id': wh_id,
                            'warehouse_name': wh_name,
                            'warehouse_office_id': wh_office_id,
                            'chrtId': s.get('chrtId'),
                            'amount': s.get('amount', 0)
                        })
                    time.sleep(0.5)

            self._log_ok('get_stocks_wh', date)
            return all_stocks
        except Exception as e:
            return self._log_err('get_stocks_wh', date, e)

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
                self.clickhouse = make_db(self.subd, self.bot_token, self.chat_list, self.message_type, self.host, self.port, self.username, self.password,
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
