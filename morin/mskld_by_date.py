from .common import Common
from .clickhouse import Clickhouse
import requests
from datetime import datetime, timedelta
import clickhouse_connect
import pandas as pd
import os
from dateutil import parser
import time
import hashlib
from io import StringIO
import json


class MSKLDbyDate:
    def __init__(self, bot_token: str = '', chats: str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', token: str = '', start: str = '', backfill_days: int = 0, reports: str = ''):
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
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.platform = 'mskld'
        self.err429 = False
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
            "Authorization": f"Bearer {token}"
        }

        self.source_dict = {
            # Справочники
            'entity_assortment': {
                'platform': 'mskld',
                'report_name': 'entity_assortment',
                'upload_table': 'entity_assortment',
                'func_name': self.get_entity_assortment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_assortment_history': {
                'platform': 'mskld',
                'report_name': 'entity_assortment_history',
                'upload_table': 'entity_assortment_history',
                'func_name': self.get_entity_assortment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_bonustransaction': {
                'platform': 'mskld',
                'report_name': 'entity_bonustransaction',
                'upload_table': 'entity_bonustransaction',
                'func_name': self.get_entity_bonustransaction,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_bonustransaction_history': {
                'platform': 'mskld',
                'report_name': 'entity_bonustransaction_history',
                'upload_table': 'entity_bonustransaction_history',
                'func_name': self.get_entity_bonustransaction,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_bonusprogram': {
                'platform': 'mskld',
                'report_name': 'entity_bonusprogram',
                'upload_table': 'entity_bonusprogram',
                'func_name': self.get_entity_bonusprogram,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_bonusprogram_history': {
                'platform': 'mskld',
                'report_name': 'entity_bonusprogram_history',
                'upload_table': 'entity_bonusprogram_history',
                'func_name': self.get_entity_bonusprogram,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_currency': {
                'platform': 'mskld',
                'report_name': 'entity_currency',
                'upload_table': 'entity_currency',
                'func_name': self.get_entity_currency,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_currency_history': {
                'platform': 'mskld',
                'report_name': 'entity_currency_history',
                'upload_table': 'entity_currency_history',
                'func_name': self.get_entity_currency,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_webhook': {
                'platform': 'mskld',
                'report_name': 'entity_webhook',
                'upload_table': 'entity_webhook',
                'func_name': self.get_entity_webhook,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_webhook_history': {
                'platform': 'mskld',
                'report_name': 'entity_webhook_history',
                'upload_table': 'entity_webhook_history',
                'func_name': self.get_entity_webhook,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_productfolder': {
                'platform': 'mskld',
                'report_name': 'entity_productfolder',
                'upload_table': 'entity_productfolder',
                'func_name': self.get_entity_productfolder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_productfolder_history': {
                'platform': 'mskld',
                'report_name': 'entity_productfolder_history',
                'upload_table': 'entity_productfolder_history',
                'func_name': self.get_entity_productfolder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_contract': {
                'platform': 'mskld',
                'report_name': 'entity_contract',
                'upload_table': 'entity_contract',
                'func_name': self.get_entity_contract,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_contract_history': {
                'platform': 'mskld',
                'report_name': 'entity_contract_history',
                'upload_table': 'entity_contract_history',
                'func_name': self.get_entity_contract,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_uom': {
                'platform': 'mskld',
                'report_name': 'entity_uom',
                'upload_table': 'entity_uom',
                'func_name': self.get_entity_uom,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_uom_history': {
                'platform': 'mskld',
                'report_name': 'entity_uom_history',
                'upload_table': 'entity_uom_history',
                'func_name': self.get_entity_uom,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_task': {
                'platform': 'mskld',
                'report_name': 'entity_task',
                'upload_table': 'entity_task',
                'func_name': self.get_entity_task,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_task_history': {
                'platform': 'mskld',
                'report_name': 'entity_task_history',
                'upload_table': 'entity_task_history',
                'func_name': self.get_entity_task,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_saleschannel': {
                'platform': 'mskld',
                'report_name': 'entity_saleschannel',
                'upload_table': 'entity_saleschannel',
                'func_name': self.get_entity_saleschannel,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_saleschannel_history': {
                'platform': 'mskld',
                'report_name': 'entity_saleschannel_history',
                'upload_table': 'entity_saleschannel_history',
                'func_name': self.get_entity_saleschannel,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_bundle': {
                'platform': 'mskld',
                'report_name': 'entity_bundle',
                'upload_table': 'entity_bundle',
                'func_name': self.get_entity_bundle,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_bundle_history': {
                'platform': 'mskld',
                'report_name': 'entity_bundle_history',
                'upload_table': 'entity_bundle_history',
                'func_name': self.get_entity_bundle,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_counterparty': {
                'platform': 'mskld',
                'report_name': 'entity_counterparty',
                'upload_table': 'entity_counterparty',
                'func_name': self.get_entity_counterparty,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_counterparty_history': {
                'platform': 'mskld',
                'report_name': 'entity_counterparty_history',
                'upload_table': 'entity_counterparty_history',
                'func_name': self.get_entity_counterparty,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_variant': {
                'platform': 'mskld',
                'report_name': 'entity_variant',
                'upload_table': 'entity_variant',
                'func_name': self.get_entity_variant,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_variant_history': {
                'platform': 'mskld',
                'report_name': 'entity_variant_history',
                'upload_table': 'entity_variant_history',
                'func_name': self.get_entity_variant,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_group': {
                'platform': 'mskld',
                'report_name': 'entity_group',
                'upload_table': 'entity_group',
                'func_name': self.get_entity_group,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_group_history': {
                'platform': 'mskld',
                'report_name': 'entity_group_history',
                'upload_table': 'entity_group_history',
                'func_name': self.get_entity_group,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_role': {
                'platform': 'mskld',
                'report_name': 'entity_role',
                'upload_table': 'entity_role',
                'func_name': self.get_entity_role,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_role_history': {
                'platform': 'mskld',
                'report_name': 'entity_role_history',
                'upload_table': 'entity_role_history',
                'func_name': self.get_entity_role,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_project': {
                'platform': 'mskld',
                'report_name': 'entity_project',
                'upload_table': 'entity_project',
                'func_name': self.get_entity_project,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_project_history': {
                'platform': 'mskld',
                'report_name': 'entity_project_history',
                'upload_table': 'entity_project_history',
                'func_name': self.get_entity_project,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_region': {
                'platform': 'mskld',
                'report_name': 'entity_region',
                'upload_table': 'entity_region',
                'func_name': self.get_entity_region,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_region_history': {
                'platform': 'mskld',
                'report_name': 'entity_region_history',
                'upload_table': 'entity_region_history',
                'func_name': self.get_entity_region,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_consignment': {
                'platform': 'mskld',
                'report_name': 'entity_consignment',
                'upload_table': 'entity_consignment',
                'func_name': self.get_entity_consignment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_consignment_history': {
                'platform': 'mskld',
                'report_name': 'entity_consignment_history',
                'upload_table': 'entity_consignment_history',
                'func_name': self.get_entity_consignment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_discount': {
                'platform': 'mskld',
                'report_name': 'entity_discount',
                'upload_table': 'entity_discount',
                'func_name': self.get_entity_discount,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_discount_history': {
                'platform': 'mskld',
                'report_name': 'entity_discount_history',
                'upload_table': 'entity_discount_history',
                'func_name': self.get_entity_discount,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_store': {
                'platform': 'mskld',
                'report_name': 'entity_store',
                'upload_table': 'entity_store',
                'func_name': self.get_entity_store,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_store_history': {
                'platform': 'mskld',
                'report_name': 'entity_store_history',
                'upload_table': 'entity_store_history',
                'func_name': self.get_entity_store,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_employee': {
                'platform': 'mskld',
                'report_name': 'entity_employee',
                'upload_table': 'entity_employee',
                'func_name': self.get_entity_employee,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_employee_history': {
                'platform': 'mskld',
                'report_name': 'entity_employee_history',
                'upload_table': 'entity_employee_history',
                'func_name': self.get_entity_employee,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_expenseitem': {
                'platform': 'mskld',
                'report_name': 'entity_expenseitem',
                'upload_table': 'entity_expenseitem',
                'func_name': self.get_entity_expenseitem,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_expenseitem_history': {
                'platform': 'mskld',
                'report_name': 'entity_expenseitem_history',
                'upload_table': 'entity_expenseitem_history',
                'func_name': self.get_entity_expenseitem,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_country': {
                'platform': 'mskld',
                'report_name': 'entity_country',
                'upload_table': 'entity_country',
                'func_name': self.get_entity_country,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_country_history': {
                'platform': 'mskld',
                'report_name': 'entity_country_history',
                'upload_table': 'entity_country_history',
                'func_name': self.get_entity_country,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_product': {
                'platform': 'mskld',
                'report_name': 'entity_product',
                'upload_table': 'entity_product',
                'func_name': self.get_entity_product,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_product_history': {
                'platform': 'mskld',
                'report_name': 'entity_product_history',
                'upload_table': 'entity_product_history',
                'func_name': self.get_entity_product,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retailstore': {
                'platform': 'mskld',
                'report_name': 'entity_retailstore',
                'upload_table': 'entity_retailstore',
                'func_name': self.get_entity_retailstore,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retailstore_history': {
                'platform': 'mskld',
                'report_name': 'entity_retailstore_history',
                'upload_table': 'entity_retailstore_history',
                'func_name': self.get_entity_retailstore,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_service': {
                'platform': 'mskld',
                'report_name': 'entity_service',
                'upload_table': 'entity_service',
                'func_name': self.get_entity_service,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_service_history': {
                'platform': 'mskld',
                'report_name': 'entity_service_history',
                'upload_table': 'entity_service_history',
                'func_name': self.get_entity_service,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_organization': {
                'platform': 'mskld',
                'report_name': 'entity_organization',
                'upload_table': 'entity_organization',
                'func_name': self.get_entity_organization,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_organization_history': {
                'platform': 'mskld',
                'report_name': 'entity_organization_history',
                'upload_table': 'entity_organization_history',
                'func_name': self.get_entity_organization,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            # Документы
            'entity_retaildrawercashin': {
                'platform': 'mskld',
                'report_name': 'entity_retaildrawercashin',
                'upload_table': 'entity_retaildrawercashin',
                'func_name': self.get_entity_retaildrawercashin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retaildrawercashin_history': {
                'platform': 'mskld',
                'report_name': 'entity_retaildrawercashin_history',
                'upload_table': 'entity_retaildrawercashin_history',
                'func_name': self.get_entity_retaildrawercashin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_internalorder': {
                'platform': 'mskld',
                'report_name': 'entity_internalorder',
                'upload_table': 'entity_internalorder',
                'func_name': self.get_entity_internalorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_internalorder_history': {
                'platform': 'mskld',
                'report_name': 'entity_internalorder_history',
                'upload_table': 'entity_internalorder_history',
                'func_name': self.get_entity_internalorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_salesreturn': {
                'platform': 'mskld',
                'report_name': 'entity_salesreturn',
                'upload_table': 'entity_salesreturn',
                'func_name': self.get_entity_salesreturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_salesreturn_history': {
                'platform': 'mskld',
                'report_name': 'entity_salesreturn_history',
                'upload_table': 'entity_salesreturn_history',
                'func_name': self.get_entity_salesreturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_purchasereturn': {
                'platform': 'mskld',
                'report_name': 'entity_purchasereturn',
                'upload_table': 'entity_purchasereturn',
                'func_name': self.get_entity_purchasereturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_purchasereturn_history': {
                'platform': 'mskld',
                'report_name': 'entity_purchasereturn_history',
                'upload_table': 'entity_purchasereturn_history',
                'func_name': self.get_entity_purchasereturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_prepaymentreturn': {
                'platform': 'mskld',
                'report_name': 'entity_prepaymentreturn',
                'upload_table': 'entity_prepaymentreturn',
                'func_name': self.get_entity_prepaymentreturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_prepaymentreturn_history': {
                'platform': 'mskld',
                'report_name': 'entity_prepaymentreturn_history',
                'upload_table': 'entity_prepaymentreturn_history',
                'func_name': self.get_entity_prepaymentreturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_paymentin': {
                'platform': 'mskld',
                'report_name': 'entity_paymentin',
                'upload_table': 'entity_paymentin',
                'func_name': self.get_entity_paymentin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_paymentin_history': {
                'platform': 'mskld',
                'report_name': 'entity_paymentin_history',
                'upload_table': 'entity_paymentin_history',
                'func_name': self.get_entity_paymentin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_commissionreportout': {
                'platform': 'mskld',
                'report_name': 'entity_commissionreportout',
                'upload_table': 'entity_commissionreportout',
                'func_name': self.get_entity_commissionreportout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_commissionreportout_history': {
                'platform': 'mskld',
                'report_name': 'entity_commissionreportout_history',
                'upload_table': 'entity_commissionreportout_history',
                'func_name': self.get_entity_commissionreportout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retaildrawercashout': {
                'platform': 'mskld',
                'report_name': 'entity_retaildrawercashout',
                'upload_table': 'entity_retaildrawercashout',
                'func_name': self.get_entity_retaildrawercashout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retaildrawercashout_history': {
                'platform': 'mskld',
                'report_name': 'entity_retaildrawercashout_history',
                'upload_table': 'entity_retaildrawercashout_history',
                'func_name': self.get_entity_retaildrawercashout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processingorder': {
                'platform': 'mskld',
                'report_name': 'entity_processingorder',
                'upload_table': 'entity_processingorder',
                'func_name': self.get_entity_processingorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processingorder_history': {
                'platform': 'mskld',
                'report_name': 'entity_processingorder_history',
                'upload_table': 'entity_processingorder_history',
                'func_name': self.get_entity_processingorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_customerorder': {
                'platform': 'mskld',
                'report_name': 'entity_customerorder',
                'upload_table': 'entity_customerorder',
                'func_name': self.get_entity_customerorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_customerorder_history': {
                'platform': 'mskld',
                'report_name': 'entity_customerorder_history',
                'upload_table': 'entity_customerorder_history',
                'func_name': self.get_entity_customerorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_purchaseorder': {
                'platform': 'mskld',
                'report_name': 'entity_purchaseorder',
                'upload_table': 'entity_purchaseorder',
                'func_name': self.get_entity_purchaseorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_purchaseorder_history': {
                'platform': 'mskld',
                'report_name': 'entity_purchaseorder_history',
                'upload_table': 'entity_purchaseorder_history',
                'func_name': self.get_entity_purchaseorder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_inventory': {
                'platform': 'mskld',
                'report_name': 'entity_inventory',
                'upload_table': 'entity_inventory',
                'func_name': self.get_entity_inventory,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_inventory_history': {
                'platform': 'mskld',
                'report_name': 'entity_inventory_history',
                'upload_table': 'entity_inventory_history',
                'func_name': self.get_entity_inventory,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_paymentout': {
                'platform': 'mskld',
                'report_name': 'entity_paymentout',
                'upload_table': 'entity_paymentout',
                'func_name': self.get_entity_paymentout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_paymentout_history': {
                'platform': 'mskld',
                'report_name': 'entity_paymentout_history',
                'upload_table': 'entity_paymentout_history',
                'func_name': self.get_entity_paymentout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_counterpartyadjustment': {
                'platform': 'mskld',
                'report_name': 'entity_counterpartyadjustment',
                'upload_table': 'entity_counterpartyadjustment',
                'func_name': self.get_entity_counterpartyadjustment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_counterpartyadjustment_history': {
                'platform': 'mskld',
                'report_name': 'entity_counterpartyadjustment_history',
                'upload_table': 'entity_counterpartyadjustment_history',
                'func_name': self.get_entity_counterpartyadjustment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_enter': {
                'platform': 'mskld',
                'report_name': 'entity_enter',
                'upload_table': 'entity_enter',
                'func_name': self.get_entity_enter,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_enter_history': {
                'platform': 'mskld',
                'report_name': 'entity_enter_history',
                'upload_table': 'entity_enter_history',
                'func_name': self.get_entity_enter,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_demand': {
                'platform': 'mskld',
                'report_name': 'entity_demand',
                'upload_table': 'entity_demand',
                'func_name': self.get_entity_demand,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_demand_history': {
                'platform': 'mskld',
                'report_name': 'entity_demand_history',
                'upload_table': 'entity_demand_history',
                'func_name': self.get_entity_demand,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_move': {
                'platform': 'mskld',
                'report_name': 'entity_move',
                'upload_table': 'entity_move',
                'func_name': self.get_entity_move,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_move_history': {
                'platform': 'mskld',
                'report_name': 'entity_move_history',
                'upload_table': 'entity_move_history',
                'func_name': self.get_entity_move,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_commissionreportin': {
                'platform': 'mskld',
                'report_name': 'entity_commissionreportin',
                'upload_table': 'entity_commissionreportin',
                'func_name': self.get_entity_commissionreportin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_commissionreportin_history': {
                'platform': 'mskld',
                'report_name': 'entity_commissionreportin_history',
                'upload_table': 'entity_commissionreportin_history',
                'func_name': self.get_entity_commissionreportin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_pricelist': {
                'platform': 'mskld',
                'report_name': 'entity_pricelist',
                'upload_table': 'entity_pricelist',
                'func_name': self.get_entity_pricelist,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_pricelist_history': {
                'platform': 'mskld',
                'report_name': 'entity_pricelist_history',
                'upload_table': 'entity_pricelist_history',
                'func_name': self.get_entity_pricelist,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_prepayment': {
                'platform': 'mskld',
                'report_name': 'entity_prepayment',
                'upload_table': 'entity_prepayment',
                'func_name': self.get_entity_prepayment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_prepayment_history': {
                'platform': 'mskld',
                'report_name': 'entity_prepayment_history',
                'upload_table': 'entity_prepayment_history',
                'func_name': self.get_entity_prepayment,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_supply': {
                'platform': 'mskld',
                'report_name': 'entity_supply',
                'upload_table': 'entity_supply',
                'func_name': self.get_entity_supply,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_supply_history': {
                'platform': 'mskld',
                'report_name': 'entity_supply_history',
                'upload_table': 'entity_supply_history',
                'func_name': self.get_entity_supply,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_cashin': {
                'platform': 'mskld',
                'report_name': 'entity_cashin',
                'upload_table': 'entity_cashin',
                'func_name': self.get_entity_cashin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_cashin_history': {
                'platform': 'mskld',
                'report_name': 'entity_cashin_history',
                'upload_table': 'entity_cashin_history',
                'func_name': self.get_entity_cashin,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_cashout': {
                'platform': 'mskld',
                'report_name': 'entity_cashout',
                'upload_table': 'entity_cashout',
                'func_name': self.get_entity_cashout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_cashout_history': {
                'platform': 'mskld',
                'report_name': 'entity_cashout_history',
                'upload_table': 'entity_cashout_history',
                'func_name': self.get_entity_cashout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retaildemand': {
                'platform': 'mskld',
                'report_name': 'entity_retaildemand',
                'upload_table': 'entity_retaildemand',
                'func_name': self.get_entity_retaildemand,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retaildemand_history': {
                'platform': 'mskld',
                'report_name': 'entity_retaildemand_history',
                'upload_table': 'entity_retaildemand_history',
                'func_name': self.get_entity_retaildemand,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retailshift': {
                'platform': 'mskld',
                'report_name': 'entity_retailshift',
                'upload_table': 'entity_retailshift',
                'func_name': self.get_entity_retailshift,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retailshift_history': {
                'platform': 'mskld',
                'report_name': 'entity_retailshift_history',
                'upload_table': 'entity_retailshift_history',
                'func_name': self.get_entity_retailshift,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retailsalesreturn': {
                'platform': 'mskld',
                'report_name': 'entity_retailsalesreturn',
                'upload_table': 'entity_retailsalesreturn',
                'func_name': self.get_entity_retailsalesreturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_retailsalesreturn_history': {
                'platform': 'mskld',
                'report_name': 'entity_retailsalesreturn_history',
                'upload_table': 'entity_retailsalesreturn_history',
                'func_name': self.get_entity_retailsalesreturn,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_loss': {
                'platform': 'mskld',
                'report_name': 'entity_loss',
                'upload_table': 'entity_loss',
                'func_name': self.get_entity_loss,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_loss_history': {
                'platform': 'mskld',
                'report_name': 'entity_loss_history',
                'upload_table': 'entity_loss_history',
                'func_name': self.get_entity_loss,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_invoiceout': {
                'platform': 'mskld',
                'report_name': 'entity_invoiceout',
                'upload_table': 'entity_invoiceout',
                'func_name': self.get_entity_invoiceout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_invoiceout_history': {
                'platform': 'mskld',
                'report_name': 'entity_invoiceout_history',
                'upload_table': 'entity_invoiceout_history',
                'func_name': self.get_entity_invoiceout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_invoicein': {
                'platform': 'mskld',
                'report_name': 'entity_invoicein',
                'upload_table': 'entity_invoicein',
                'func_name': self.get_entity_invoicein,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_invoicein_history': {
                'platform': 'mskld',
                'report_name': 'entity_invoicein_history',
                'upload_table': 'entity_invoicein_history',
                'func_name': self.get_entity_invoicein,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_factureout': {
                'platform': 'mskld',
                'report_name': 'entity_factureout',
                'upload_table': 'entity_factureout',
                'func_name': self.get_entity_factureout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_factureout_history': {
                'platform': 'mskld',
                'report_name': 'entity_factureout_history',
                'upload_table': 'entity_factureout_history',
                'func_name': self.get_entity_factureout,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_facturein': {
                'platform': 'mskld',
                'report_name': 'entity_facturein',
                'upload_table': 'entity_facturein',
                'func_name': self.get_entity_facturein,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_facturein_history': {
                'platform': 'mskld',
                'report_name': 'entity_facturein_history',
                'upload_table': 'entity_facturein_history',
                'func_name': self.get_entity_facturein,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processingplan': {
                'platform': 'mskld',
                'report_name': 'entity_processingplan',
                'upload_table': 'entity_processingplan',
                'func_name': self.get_entity_processingplan,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processingplan_history': {
                'platform': 'mskld',
                'report_name': 'entity_processingplan_history',
                'upload_table': 'entity_processingplan_history',
                'func_name': self.get_entity_processingplan,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processingplanfolder': {
                'platform': 'mskld',
                'report_name': 'entity_processingplanfolder',
                'upload_table': 'entity_processingplanfolder',
                'func_name': self.get_entity_processingplanfolder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processingplanfolder_history': {
                'platform': 'mskld',
                'report_name': 'entity_processingplanfolder_history',
                'upload_table': 'entity_processingplanfolder_history',
                'func_name': self.get_entity_processingplanfolder,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processing': {
                'platform': 'mskld',
                'report_name': 'entity_processing',
                'upload_table': 'entity_processing',
                'func_name': self.get_entity_processing,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'entity_processing_history': {
                'platform': 'mskld',
                'report_name': 'entity_processing_history',
                'upload_table': 'entity_processing_history',
                'func_name': self.get_entity_processing,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            # Отчеты
            'report_stock_all': {
                'platform': 'mskld',
                'report_name': 'report_stock_all',
                'upload_table': 'report_stock_all',
                'func_name': self.get_report_stock_all,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_all_moment': {
                'platform': 'mskld',
                'report_name': 'report_stock_all_moment',
                'upload_table': 'report_stock_all_moment',
                'func_name': self.get_report_stock_all_moment,
                'uniq_columns': 'timeStamp',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_all_history': {
                'platform': 'mskld',
                'report_name': 'report_stock_all_history',
                'upload_table': 'report_stock_all_history',
                'func_name': self.get_report_stock_all,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_bystore': {
                'platform': 'mskld',
                'report_name': 'report_stock_bystore',
                'upload_table': 'report_stock_bystore',
                'func_name': self.get_report_stock_bystore,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_bystore_moment': {
                'platform': 'mskld',
                'report_name': 'report_stock_bystore_moment',
                'upload_table': 'report_stock_bystore_moment',
                'func_name': self.get_report_stock_bystore_moment,
                'uniq_columns': 'timeStamp',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_bystore_history': {
                'platform': 'mskld',
                'report_name': 'report_stock_bystore_history',
                'upload_table': 'report_stock_bystore_history',
                'func_name': self.get_report_stock_bystore,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_all_current': {
                'platform': 'mskld',
                'report_name': 'report_stock_all_current',
                'upload_table': 'report_stock_all_current',
                'func_name': self.get_report_stock_all_current,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_all_current_moment': {
                'platform': 'mskld',
                'report_name': 'report_stock_all_current_moment',
                'upload_table': 'report_stock_all_current_moment',
                'func_name': self.get_report_stock_all_current_moment,
                'uniq_columns': 'timeStamp',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',
                'delay': 20
            },
            'report_stock_all_current_history': {
                'platform': 'mskld',
                'report_name': 'report_stock_all_current_history',
                'upload_table': 'report_stock_all_current_history',
                'func_name': self.get_report_stock_all_current,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_byproduct': {
                'platform': 'mskld',
                'report_name': 'report_profit_byproduct',
                'upload_table': 'report_profit_byproduct',
                'func_name': self.get_report_profit_byproduct,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_byproduct_history': {
                'platform': 'mskld',
                'report_name': 'report_profit_byproduct_history',
                'upload_table': 'report_profit_byproduct_history',
                'func_name': self.get_report_profit_byproduct,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_byvariant': {
                'platform': 'mskld',
                'report_name': 'report_profit_byvariant',
                'upload_table': 'report_profit_byvariant',
                'func_name': self.get_report_profit_byvariant,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_byvariant_history': {
                'platform': 'mskld',
                'report_name': 'report_profit_byvariant_history',
                'upload_table': 'report_profit_byvariant_history',
                'func_name': self.get_report_profit_byvariant,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_byemployee': {
                'platform': 'mskld',
                'report_name': 'report_profit_byemployee',
                'upload_table': 'report_profit_byemployee',
                'func_name': self.get_report_profit_byemployee,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_byemployee_history': {
                'platform': 'mskld',
                'report_name': 'report_profit_byemployee_history',
                'upload_table': 'report_profit_byemployee_history',
                'func_name': self.get_report_profit_byemployee,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_bycounterparty': {
                'platform': 'mskld',
                'report_name': 'report_profit_bycounterparty',
                'upload_table': 'report_profit_bycounterparty',
                'func_name': self.get_report_profit_bycounterparty,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_bycounterparty_history': {
                'platform': 'mskld',
                'report_name': 'report_profit_bycounterparty_history',
                'upload_table': 'report_profit_bycounterparty_history',
                'func_name': self.get_report_profit_bycounterparty,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_bysaleschannel': {
                'platform': 'mskld',
                'report_name': 'report_profit_bysaleschannel',
                'upload_table': 'report_profit_bysaleschannel',
                'func_name': self.get_report_profit_bysaleschannel,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_profit_bysaleschannel_history': {
                'platform': 'mskld',
                'report_name': 'report_profit_bysaleschannel_history',
                'upload_table': 'report_profit_bysaleschannel_history',
                'func_name': self.get_report_profit_bysaleschannel,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_money_byaccount': {
                'platform': 'mskld',
                'report_name': 'report_money_byaccount',
                'upload_table': 'report_money_byaccount',
                'func_name': self.get_report_money_byaccount,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_money_byaccount_moment': {
                'platform': 'mskld',
                'report_name': 'report_money_byaccount_moment',
                'upload_table': 'report_money_byaccount_moment',
                'func_name': self.get_report_money_byaccount_moment,
                'uniq_columns': 'timeStamp',
                'partitions': 'date',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_date',
                'history': True,
                'frequency': 'daily',
                'delay': 20
            },
            'report_money_byaccount_history': {
                'platform': 'mskld',
                'report_name': 'report_money_byaccount_history',
                'upload_table': 'report_money_byaccount_history',
                'func_name': self.get_report_money_byaccount,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_money_plotseries': {
                'platform': 'mskld',
                'report_name': 'report_money_plotseries',
                'upload_table': 'report_money_plotseries',
                'func_name': self.get_report_money_plotseries,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_money_plotseries_history': {
                'platform': 'mskld',
                'report_name': 'report_money_plotseries_history',
                'upload_table': 'report_money_plotseries_history',
                'func_name': self.get_report_money_plotseries,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_turnover_all': {
                'platform': 'mskld',
                'report_name': 'report_turnover_all',
                'upload_table': 'report_turnover_all',
                'func_name': self.get_report_turnover_all,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'report_turnover_all_history': {
                'platform': 'mskld',
                'report_name': 'report_turnover_all_history',
                'upload_table': 'report_turnover_all_history',
                'func_name': self.get_report_turnover_all,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },

            'audit': {
                'platform': 'mskld',
                'report_name': 'audit',
                'upload_table': 'audit',
                'func_name': self.get_audit,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'audit_history': {
                'platform': 'mskld',
                'report_name': 'audit_history',
                'upload_table': 'audit_history',
                'func_name': self.get_audit,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'notification': {
                'platform': 'mskld',
                'report_name': 'notification',
                'upload_table': 'notification',
                'func_name': self.get_notification,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'notification_history': {
                'platform': 'mskld',
                'report_name': 'notification_history',
                'upload_table': 'notification_history',
                'func_name': self.get_notification,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'notification_subscription': {
                'platform': 'mskld',
                'report_name': 'notification_subscription',
                'upload_table': 'notification_subscription',
                'func_name': self.get_notification_subscription,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            },
            'notification_subscription_history': {
                'platform': 'mskld',
                'report_name': 'notification_subscription_history',
                'upload_table': 'notification_subscription_history',
                'func_name': self.get_notification_subscription,
                'uniq_columns': 'timeStamp',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'nothing',
                'history': False,
                'frequency': 'daily',
                'delay': 20
            }
        }

    def get_entity_size(self, entity_path: str) -> int:
        """Получение количества записей в сущности"""
        try:
            url = f"{self.base_url}/{entity_path}"
            params = {"limit": "1000", "offset": "0"}
            response = requests.get(url, headers=self.headers, params=params)
            code = response.status_code

            if code == 429:
                self.err429 = True
                time.sleep(1)
                return self.get_entity_size(entity_path)

            if code == 200:
                data = response.json()
                return data.get("meta", {}).get("size", 0)
            else:
                response.raise_for_status()

        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Функция: get_entity_size. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_data_batch(self, entity_path: str, offset: int, limit: int = 1000) -> list:
        """Получение батча данных"""
        try:
            url = f"{self.base_url}/{entity_path}"
            params = {"limit": str(limit), "offset": str(offset)}
            response = requests.get(url, headers=self.headers, params=params)
            code = response.status_code

            if code == 429:
                self.err429 = True
                time.sleep(1)
                return self.get_data_batch(entity_path, offset, limit)

            if code == 200:
                data = response.json()
                return data.get("rows", [])
            else:
                response.raise_for_status()

        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Функция: get_data_batch. Offset: {offset}. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_all_data(self, entity_path: str, start_offset: int = 0) -> list:
        """Получение всех данных из МойСклад"""
        try:
            # Получаем общий размер данных
            total_size = self.get_entity_size(entity_path)
            if total_size == 0:
                return []

            # Собираем все данные
            all_data = []
            limit = 1000
            current_offset = start_offset

            while current_offset < total_size:
                batch_data = self.get_data_batch(entity_path, current_offset, limit)
                if batch_data:
                    all_data.extend(batch_data)
                    current_offset += limit
                    time.sleep(1)  # Пауза между запросами
                else:
                    break

            return self.common.spread_table(self.common.spread_table(self.common.spread_table(all_data)))

        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Функция: get_all_data. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    # Все функции получения данных - примитивные, без фильтрации по датам
    def get_entity_enter(self, date=''):
        try:
            final_result = self.get_all_data("entity/enter")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_enter. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_enter. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_counterpartyadjustment(self, date=''):
        try:
            final_result = self.get_all_data("entity/counterpartyadjustment")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_counterpartyadjustment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_counterpartyadjustment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_paymentout(self, date=''):
        try:
            final_result = self.get_all_data("entity/paymentout")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_paymentout. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_paymentout. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_inventory(self, date=''):
        try:
            final_result = self.get_all_data("entity/inventory")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_inventory. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_inventory. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_purchaseorder(self, date=''):
        try:
            final_result = self.get_all_data("entity/purchaseorder")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_purchaseorder. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_purchaseorder. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_customerorder(self, date=''):
        try:
            final_result = self.get_all_data("entity/customerorder")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_customerorder. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_customerorder. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_processingorder(self, date=''):
        try:
            final_result = self.get_all_data("entity/processingorder")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processingorder. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processingorder. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_retaildrawercashout(self, date=''):
        try:
            final_result = self.get_all_data("entity/retaildrawercashout")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retaildrawercashout. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retaildrawercashout. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_commissionreportout(self, date=''):
        try:
            final_result = self.get_all_data("entity/commissionreportout")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_commissionreportout. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_commissionreportout. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_paymentin(self, date=''):
        try:
            final_result = self.get_all_data("entity/paymentin")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_paymentin. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_paymentin. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_prepaymentreturn(self, date=''):
        try:
            final_result = self.get_all_data("entity/prepaymentreturn")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_prepaymentreturn. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_prepaymentreturn. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_entity_assortment(self, date=''):
        try:
            final_result = self.get_all_data("entity/assortment")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_assortment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_assortment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_bonustransaction(self, date=''):
        try:
            final_result = self.get_all_data("entity/bonustransaction")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_bonustransaction. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_bonustransaction. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_bonusprogram(self, date=''):
        try:
            final_result = self.get_all_data("entity/bonusprogram")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_bonusprogram. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_bonusprogram. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_currency(self, date=''):
        try:
            final_result = self.get_all_data("entity/currency")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_currency. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_currency. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_webhook(self, date=''):
        try:
            final_result = self.get_all_data("entity/webhook")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_webhook. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_webhook. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_productfolder(self, date=''):
        try:
            final_result = self.get_all_data("entity/productfolder")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_productfolder. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_productfolder. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_contract(self, date=''):
        try:
            final_result = self.get_all_data("entity/contract")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_contract. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_contract. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_uom(self, date=''):
        try:
            final_result = self.get_all_data("entity/uom")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_uom. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_uom. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_task(self, date=''):
        try:
            final_result = self.get_all_data("entity/task")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_task. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_task. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_saleschannel(self, date=''):
        try:
            final_result = self.get_all_data("entity/saleschannel")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_saleschannel. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_saleschannel. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_bundle(self, date=''):
        try:
            final_result = self.get_all_data("entity/bundle")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_bundle. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_bundle. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_counterparty(self, date=''):
        try:
            final_result = self.get_all_data("entity/counterparty")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_counterparty. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_counterparty. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_variant(self, date=''):
        try:
            final_result = self.get_all_data("entity/variant")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_variant. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_variant. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_group(self, date=''):
        try:
            final_result = self.get_all_data("entity/group")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_group. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_group. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_role(self, date=''):
        try:
            final_result = self.get_all_data("entity/role")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_role. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_role. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_project(self, date=''):
        try:
            final_result = self.get_all_data("entity/project")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_project. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_project. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_region(self, date=''):
        try:
            final_result = self.get_all_data("entity/region")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_region. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_region. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_consignment(self, date=''):
        try:
            final_result = self.get_all_data("entity/consignment")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_consignment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_consignment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_discount(self, date=''):
        try:
            final_result = self.get_all_data("entity/discount")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_discount. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_discount. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_store(self, date=''):
        try:
            final_result = self.get_all_data("entity/store")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_store. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_store. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_employee(self, date=''):
        try:
            final_result = self.get_all_data("entity/employee")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_employee. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_employee. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_expenseitem(self, date=''):
        try:
            final_result = self.get_all_data("entity/expenseitem")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_expenseitem. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_expenseitem. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_country(self, date=''):
        try:
            final_result = self.get_all_data("entity/country")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_country. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_country. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_product(self, date=''):
        try:
            final_result = self.get_all_data("entity/product")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_product. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_product. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_retailstore(self, date=''):
        try:
            final_result = self.get_all_data("entity/retailstore")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retailstore. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retailstore. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_service(self, date=''):
        try:
            final_result = self.get_all_data("entity/service")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_service. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_service. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_organization(self, date=''):
        try:
            final_result = self.get_all_data("entity/organization")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_organization. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_organization. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_retaildrawercashin(self, date=''):
        try:
            final_result = self.get_all_data("entity/retaildrawercashin")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retaildrawercashin. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retaildrawercashin. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_internalorder(self, date=''):
        try:
            final_result = self.get_all_data("entity/internalorder")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_internalorder. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_internalorder. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_salesreturn(self, date=''):
        try:
            final_result = self.get_all_data("entity/salesreturn")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_salesreturn. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_salesreturn. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_purchasereturn(self, date=''):
        try:
            final_result = self.get_all_data("entity/purchasereturn")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_purchasereturn. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_purchasereturn. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_stock_all(self, date=''):
        try:
            final_result = self.get_all_data("report/stock/all")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_stock_all_moment(self, date):
        try:
            final_result = self.get_all_data(f"report/stock/all?filter=moment={date} 12:00:00")
            final_result_date = []
            for x in final_result:
                x['date'] = date
                final_result_date.append(x)
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all_moment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result_date
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all_moment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_report_stock_bystore_moment(self, date):
        try:
            final_result = self.get_all_data(f"report/stock/bystore?filter=moment={date} 12:00:00")
            final_result_date = []
            for x in final_result:
                x['date'] = date
                final_result_date.append(x)
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_bystore_moment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result_date
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_bystore_moment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_report_stock_all_current_moment(self, date):
        try:
            final_result = self.get_all_data(f"report/stock/all/current?filter=moment={date} 12:00:00")
            final_result_date = []
            for x in final_result:
                x['date'] = date
                final_result_date.append(x)
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all_current_moment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result_date
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all_current_moment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_report_money_byaccount_moment(self, date):
        try:
            final_result = self.get_all_data(f"report/money/byaccount?filter=moment={date} 12:00:00")
            final_result_date = []
            for x in final_result:
                x['date'] = date
                final_result_date.append(x)
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_money_byaccount_moment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result_date
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_money_byaccount_moment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message


    def get_audit(self, date=''):
        try:
            final_result = self.get_all_data("audit")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_audit. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_audit. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_notification(self, date=''):
        try:
            final_result = self.get_all_data("notification")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_notification. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_notification. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_notification_subscription(self, date=''):
        try:
            final_result = self.get_all_data("notification/subscription")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_notification_subscription. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_notification_subscription. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_demand(self, date=''):
        try:
            final_result = self.get_all_data("entity/demand")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_demand. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_demand. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_move(self, date=''):
        try:
            final_result = self.get_all_data("entity/move")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_move. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_move. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_commissionreportin(self, date=''):
        try:
            final_result = self.get_all_data("entity/commissionreportin")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_commissionreportin. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_commissionreportin. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_pricelist(self, date=''):
        try:
            final_result = self.get_all_data("entity/pricelist")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_pricelist. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_pricelist. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_prepayment(self, date=''):
        try:
            final_result = self.get_all_data("entity/prepayment")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_prepayment. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_prepayment. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_supply(self, date=''):
        try:
            final_result = self.get_all_data("entity/supply")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_supply. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_supply. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_cashin(self, date=''):
        try:
            final_result = self.get_all_data("entity/cashin")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_cashin. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_cashin. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_cashout(self, date=''):
        try:
            final_result = self.get_all_data("entity/cashout")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_cashout. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_cashout. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_retaildemand(self, date=''):
        try:
            final_result = self.get_all_data("entity/retaildemand")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retaildemand. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retaildemand. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_retailshift(self, date=''):
        try:
            final_result = self.get_all_data("entity/retailshift")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retailshift. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retailshift. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_retailsalesreturn(self, date=''):
        try:
            final_result = self.get_all_data("entity/retailsalesreturn")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retailsalesreturn. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_retailsalesreturn. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_loss(self, date=''):
        try:
            final_result = self.get_all_data("entity/loss")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_loss. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_loss. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_invoiceout(self, date=''):
        try:
            final_result = self.get_all_data("entity/invoiceout")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_invoiceout. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_invoiceout. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_invoicein(self, date=''):
        try:
            final_result = self.get_all_data("entity/invoicein")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_invoicein. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_invoicein. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_factureout(self, date=''):
        try:
            final_result = self.get_all_data("entity/factureout")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_factureout. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_factureout. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_facturein(self, date=''):
        try:
            final_result = self.get_all_data("entity/facturein")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_facturein. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_facturein. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_processingplan(self, date=''):
        try:
            final_result = self.get_all_data("entity/processingplan")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processingplan. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processingplan. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_processingplanfolder(self, date=''):
        try:
            final_result = self.get_all_data("entity/processingplanfolder")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processingplanfolder. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processingplanfolder. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_entity_processing(self, date=''):
        try:
            final_result = self.get_all_data("entity/processing")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processing. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_entity_processing. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_stock_bystore(self, date=''):
        try:
            final_result = self.get_all_data("report/stock/bystore")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_bystore. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_bystore. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_stock_all_current(self, date=''):
        try:
            final_result = self.get_all_data("report/stock/all/current")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all_current. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_stock_all_current. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_profit_byproduct(self, date=''):
        try:
            final_result = self.get_all_data("report/profit/byproduct")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_byproduct. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_byproduct. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_profit_byvariant(self, date=''):
        try:
            final_result = self.get_all_data("report/profit/byvariant")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_byvariant. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_byvariant. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_profit_byemployee(self, date=''):
        try:
            final_result = self.get_all_data("report/profit/byemployee")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_byemployee. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_byemployee. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_profit_bycounterparty(self, date=''):
        try:
            final_result = self.get_all_data("report/profit/bycounterparty")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_bycounterparty. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_bycounterparty. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_profit_bysaleschannel(self, date=''):
        try:
            final_result = self.get_all_data("report/profit/bysaleschannel")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_bysaleschannel. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_profit_bysaleschannel. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_money_byaccount(self, date=''):
        try:
            final_result = self.get_all_data("report/money/byaccount")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_money_byaccount. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_money_byaccount. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_money_plotseries(self, date=''):
        try:
            final_result = self.get_all_data("report/money/plotseries")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_money_plotseries. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_money_plotseries. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def get_report_turnover_all(self, date=''):
        try:
            final_result = self.get_all_data("report/turnover/all")
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_turnover_all. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return final_result
        except Exception as e:
            message = f'Платформа: MSKLD. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_report_turnover_all. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return message

    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
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
