import time
import os
import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import zipfile
import io
from .common import Common
from .clickhouse import Clickhouse
from .base_client import BaseMarketplaceClient

class OZONreklama:
    def __init__(self, bot_token:str, chat_list:str, message_type: str, subd: str, add_name: str, clientid:str, token: str , host: str, port: str, username: str, password: str, database: str, start: str, backfill_days: int):
        self.bot_token = bot_token
        self.chat_list = chat_list
        self.message_type = message_type
        self.clientid = clientid
        self.token = token
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.subd = subd
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.add_name = add_name.replace(' ','').replace('-','_')
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.yesterday = self.today - timedelta(days=1)
        self.start = start
        self.backfill_days = backfill_days
        self.err429 = False
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password, database=database)
        self.clickhouse = Clickhouse(bot_token, chat_list, message_type, host, port, username, password, database, start, self.add_name, self.err429, backfill_days, 'ozon_ads')
        self.api = BaseMarketplaceClient(
            base_url='https://api-performance.ozon.ru',
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )

    def chunk_list(self, lst, chunk_size):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    def get_token(self):
        try:
            payload = {"client_id": self.clientid, "client_secret": self.token, "grant_type": "client_credentials"}
            saved_auth = self.api.client.headers.get('Authorization', '')
            if 'Authorization' in self.api.client.headers:
                del self.api.client.headers['Authorization']
            result = self.api._request('POST', '/api/client/token', json=payload)
            access_token = result.get('access_token')
            if access_token:
                self.api.client.headers['Authorization'] = f'Bearer {access_token}'
                message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Токен получен успешно"
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
            else:
                if saved_auth:
                    self.api.client.headers['Authorization'] = saved_auth
                message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Ошибка: токен не получен"
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return access_token
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Ошибка получения токена: {str(e)}"
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def get_names(self):
        try:
            result = self.api._request('GET', '/api/client/campaign')
            message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_names. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            campaigns = result['list']
            df = pd.json_normalize(campaigns)
            df.columns = [c.replace('.', '_') for c in df.columns]
            if 'toDate' in df.columns:
                df['toDate'] = pd.to_datetime(df['toDate'], errors='coerce')
                df['toDate'] = df['toDate'].fillna(pd.Timestamp.today().normalize())
            else:
                df['toDate'] = pd.Timestamp.today().normalize()
            rows = df.to_dict('records')
            table_name = f"ozon_ads_campaigns_{self.add_name}"
            text_columns_set = self.clickhouse.ch_text_columns_set(table_name)
            self.clickhouse.create_alter_ch(rows, table_name, 'id', '', 'ReplacingMergeTree(timeStamp)')
            df = self.common.check_and_convert_types(rows, 'id', '', text_columns_set)
            self.clickhouse.ch_insert(df, table_name)
            return 200
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_names. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def text_to_df(self, response_text, date):
        pd.set_option('display.max_columns', None)
        csv_file = response_text.splitlines()
        sp_columns = ['orderDate', 'orderId', 'orderNum', 'ozonId', 'productOzonId', 'artikul', 'name', 'count', 'price', 'value', 'rate', 'cost']
        sku_columns = ['addDate', 'sku', 'productName', 'productPrice', 'views', 'clicks', 'cost', 'inBasket', 'sales', 'orders', 'modelOrders', 'modelSales', 'drr']
        banner_columns = ['banner', 'pageType', 'viewCond', 'platform', 'views', 'clicks', 'reach', 'cost']
        brand_shelf_columns = ['conditionType', 'viewCond', 'platform', 'views', 'clicks', 'reach', 'cost']
        sis_columns = ['pageType', 'views', 'clicks', 'cost', 'reach']

        replace_dict = {'SKU': 'sku', 'CTR (%)': 'ctr', 'Средняя стоимость клика, ₽': 'avgCostPerClick',
                        'Расход за минусом бонусов, ₽, с НДС': 'costNoBonus', 'Дата добавления': 'addDate',
                        'ДРР, %': 'drr',
                        'Продажи с заказов модели, ₽': 'modelSales', 'Продажи, ₽': 'sales', 'В корзину': 'inBasket',
                        'Название товара': 'productName', 'Цена товара, ₽': 'productPrice', 'Расход, ₽, с НДС': 'cost',
                        'Показы': 'views', 'Заказы': 'orders', 'Клики': 'clicks', 'Выручка, ₽': 'revenue',
                        'Заказы модели': 'modelOrders', 'Выручка с заказов модели, ₽': 'modelRevenue',
                        'Тип страницы': 'pageType', 'Охват': 'reach', 'Тип условия': 'conditionType',
                        'Условие показа': 'viewCond',
                        'Платформа': 'platform', 'Баннер': 'banner', 'Дата': 'orderDate', 'ID заказа': 'orderId',
                        'Номер заказа': 'orderNum',
                        'Ozon ID': 'ozonId', 'Ozon ID продвигаемого товара': 'productOzonId', 'Артикул': 'artikul',
                        'Наименование': 'name',
                        'Количество': 'count', 'Цена продажи': 'price', 'Стоимость, ₽': 'value', 'Ставка, ₽': 'rate',
                        'Расход, ₽': 'cost'}

        add_to_table = "unknown"
        if len(csv_file) > 1:
            csv_data = '\n'.join(csv_file[1:])
            campaign_id = int(csv_file[0].split('№')[1].split(',')[0].strip())
            date_as_date = datetime.strptime(date, '%Y-%m-%d')
            df = pd.read_csv(StringIO(csv_data), sep=';')
            first_column_name = df.columns[0]
            df_filtered = df.query(f'`{first_column_name}` != "Корректировка" and `{first_column_name}` != "Всего"')
            for key, value in replace_dict.items():
                try:
                    df_filtered = df_filtered.rename(columns={key: value})
                except:
                    pass

            if set(sp_columns).issubset(df_filtered.columns):
                add_to_table = 'sp'
                df_filtered = df_filtered[sp_columns]
            elif set(sku_columns).issubset(df_filtered.columns):
                add_to_table = 'sku'
                df_filtered = df_filtered[sku_columns]
            elif set(banner_columns).issubset(df_filtered.columns):
                add_to_table = 'banner'
                df_filtered = df_filtered[banner_columns]
            elif set(brand_shelf_columns).issubset(df_filtered.columns):
                add_to_table = 'shelf'
                df_filtered = df_filtered[brand_shelf_columns]
            elif set(sis_columns).issubset(df_filtered.columns):
                add_to_table = 'sis'
                df_filtered = df_filtered[sis_columns]

            df_filtered['date'] = date_as_date
            df_filtered['id'] = campaign_id
            for col in df_filtered.columns:
                try:
                    df_filtered[col] = df_filtered[col].astype(str).str.replace(',', '.')
                except:
                    pass
            rows = df_filtered.to_dict('records')
            return [rows, add_to_table]
        else:
            return [[], add_to_table]

    def _insert_with_auto_columns(self, rows, table_name, report_type):
        uniq_map = {
            'sp': 'date,id,orderDate,orderId,ozonId,productOzonId,artikul',
            'sku': 'date,id,sku',
            'banner': 'date,id,banner,pageType,viewCond,platform',
            'shelf': 'date,id,conditionType,viewCond,platform',
            'sis': 'date,id,pageType',
        }
        uniq_columns = uniq_map.get(report_type, 'date,id')
        text_columns_set = self.clickhouse.ch_text_columns_set(table_name)
        self.clickhouse.create_alter_ch(rows, table_name, uniq_columns, 'date', 'ReplacingMergeTree(timeStamp)')
        df = self.common.check_and_convert_types(rows, uniq_columns, 'date', text_columns_set)
        self.clickhouse.ch_insert(df, table_name)

    def get_data(self, campaigns, date):
        if self.err429:
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Ошибка 429, запрос не отправлен."
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None
        try:
            payload = {
                "campaigns": campaigns,
                "dateFrom": date,
                "dateTo": date,
                "groupBy": "NO_GROUP_BY"
            }
            result = self.api._request('POST', '/api/client/statistics', json=payload)
            report_uuid = result['UUID']
            for k in range(200):
                time.sleep(60)
                try:
                    status = self.api._request('GET', f'/api/client/statistics/{report_uuid}')
                    if status.get('state') == 'OK':
                        break
                except:
                    pass
            response = self.api._request_raw('GET', '/api/client/statistics/report', params={'UUID': report_uuid})
            if len(campaigns) == 1:
                text_df = self.text_to_df(response.text, date)
                rows = text_df[0]
                add_to_table = text_df[1]
                if len(rows) > 0:
                    table_name = f"ozon_ads_data_{add_to_table}_{self.add_name}"
                    self._insert_with_auto_columns(rows, table_name, add_to_table)
            else:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    for file_name in zip_file.namelist():
                        with zip_file.open(file_name) as file:
                            content = file.read().decode('utf-8')
                            text_df = self.text_to_df(content, date)
                            rows = text_df[0]
                            add_to_table = text_df[1]
                            if len(rows) > 0:
                                table_name = f"ozon_ads_data_{add_to_table}_{self.add_name}"
                                self._insert_with_auto_columns(rows, table_name, add_to_table)
                        time.sleep(2)
            return 200
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def get_campaigns_in_period(self, start_date):
        try:
            end_date = self.yesterday.strftime("%Y-%m-%d")
            result = self.api._request('GET', '/api/client/campaign')
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Результат: ОК"
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            campaigns = result['list']
            df = pd.json_normalize(campaigns)
            df['createdAt'] = df['createdAt'].str[:10]
            df['toDate'] = pd.to_datetime(df['toDate'], errors='coerce')
            df['toDate'].fillna(pd.Timestamp.today().normalize(), inplace=True)
            df['toDate'] = df['toDate'].dt.strftime('%Y-%m-%d')
            df['id'] = df['id'].astype('int64')
            required_columns = ['id', 'createdAt', 'toDate']
            df = df[required_columns]
            df_filtered = df[((df['createdAt'] <= end_date) & (df['toDate'] >= start_date))
                             | ((df['toDate'] >= start_date) & (df['createdAt'] <= end_date))
                             | ((df['createdAt'] >= start_date) & (df['toDate'] <= end_date))
                             | ((df['createdAt'] <= start_date) & (df['toDate'] >= end_date))]
            advert_id_list = df_filtered['id'].tolist()
            return advert_id_list
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Ошибка: {e}"
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def create_date_list(self, start_date_str, end_date_str):
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                date_list.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
            return date_list
        except Exception as e:
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Функция: create_date_list. Ошибка: {e}"
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return []

    def ozon_reklama_collector(self):
        create_table_query_collect = f"""
        CREATE TABLE IF NOT EXISTS ozon_ads_collection_{self.add_name} (
            date Date,
            campaignId UInt64,
            collect Bool
        ) ENGINE = ReplacingMergeTree(collect)
        ORDER BY (campaignId, date)
        """
        optimize_collection = f"OPTIMIZE TABLE ozon_ads_collection_{self.add_name} FINAL"

        now = datetime.now()
        yesterday = now - timedelta(days=1)
        self.client.command(create_table_query_collect)
        token = self.get_token()

        names_code = self.get_names()
        time.sleep(5)

        if names_code == 200:
            active_campaigns = self.get_campaigns_in_period(self.start)

            columns_query = f"SELECT name FROM system.columns WHERE database = '{self.database}' AND table = 'ozon_ads_campaigns_{self.add_name}'"
            existing_columns = [row[0] for row in self.client.query(columns_query).result_rows]
            has_to_date = 'toDate' in existing_columns

            if has_to_date:
                active_campaigns_query = f"""
                        SELECT id, createdAt, toDate
                        FROM ozon_ads_campaigns_{self.add_name}
                        WHERE id IN ({', '.join(map(str, active_campaigns))})
                        """
            else:
                active_campaigns_query = f"""
                        SELECT id, createdAt
                        FROM ozon_ads_campaigns_{self.add_name}
                        WHERE id IN ({', '.join(map(str, active_campaigns))})
                        """
            active_campaigns_query_result = self.client.query(active_campaigns_query)
            if has_to_date:
                df_campaigns = pd.DataFrame(active_campaigns_query_result.result_rows, columns=['id', 'createdAt', 'toDate'])
                df_campaigns['toDate'] = pd.to_datetime(df_campaigns['toDate']).dt.date
            else:
                df_campaigns = pd.DataFrame(active_campaigns_query_result.result_rows, columns=['id', 'createdAt'])
                df_campaigns['toDate'] = yesterday.date()
            df_campaigns['createdAt'] = pd.to_datetime(df_campaigns['createdAt']).dt.date

            campaigns_date_list = []
            yesterday_date = yesterday.strftime("%Y-%m-%d")
            for _, row in df_campaigns.iterrows():
                advertId = int(row['id'])
                start_date = row['createdAt'].strftime('%Y-%m-%d')
                end_date = row['toDate'].strftime('%Y-%m-%d')
                if end_date > yesterday_date:
                    end_date = yesterday_date
                if start_date < self.start:
                    start_date = self.start
                date_list = self.create_date_list(start_date, end_date)
                for date in date_list:
                    campaigns_date_list.append((datetime.strptime(date, '%Y-%m-%d').date(), advertId, False))
            df_active_dates = pd.DataFrame(campaigns_date_list, columns=['date', 'campaignId', 'collect'])

            self.client.insert(f'ozon_ads_collection_{self.add_name}', [tuple(x) for x in df_active_dates.to_numpy()], column_names=df_active_dates.columns.tolist())
            self.client.command(optimize_collection)
            time.sleep(10)

            false_dates_query = f"""
                    SELECT distinct date
                    FROM ozon_ads_collection_{self.add_name}
                    WHERE collect = False"""
            collect_days_rows = self.client.query(false_dates_query).result_rows
            collect_days = [item[0] for item in collect_days_rows]
            n_days_ago = now - timedelta(days=self.backfill_days)

            for day in collect_days:
                if self.err429 == False:
                    self.get_token()
                    difference = n_days_ago.date() - day
                    sql_date = day.strftime('%Y-%m-%d')
                    false_campaigns_by_date_query = f"""
                            SELECT campaignId
                            FROM ozon_ads_collection_{self.add_name}
                            WHERE collect = False AND date = '{sql_date}'"""
                    campaigns_to_collect_rows = self.client.query(false_campaigns_by_date_query).result_rows
                    campaigns_to_collect = list(set([str(item[0]) for item in campaigns_to_collect_rows]))

                    for chunk in self.chunk_list(campaigns_to_collect, 10):
                        body = list(chunk)
                        success_list = []
                        for campaign in chunk:
                            if difference.days >= 0:
                                success_list.append((day, int(campaign), True))
                        message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Кампании: {str(body)}. Начало загрузки.'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)

                        try:
                            self.get_token()
                            ozon_json = self.get_data(body, sql_date)
                            df_success = pd.DataFrame(success_list, columns=['date', 'campaignId', 'collect'])
                            if ozon_json == 200:
                                self.client.insert(f'ozon_ads_collection_{self.add_name}', [tuple(x) for x in df_success.to_numpy()], column_names=df_success.columns.tolist())
                                message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Кампании: {str(body)}. Результат: ОК."
                                self.common.log_func(self.bot_token, self.chat_list, message, 2)
                                self.client.command(optimize_collection)
                            if self.err429 == False:
                                time.sleep(2)
                        except Exception as e:
                            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Кампании: {str(body)}. Ошибка: {str(e)}."
                            self.common.log_func(self.bot_token, self.chat_list, message, 3)
                    if self.err429 == False:
                        time.sleep(10)

        self.client.command(optimize_collection)
        time.sleep(10)
