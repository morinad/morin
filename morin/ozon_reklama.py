import sys
import requests
import time
import json
import os
import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import zipfile
import io
from .common import Common

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
        self.yesterday = self.today - timedelta(days = 1)
        self.start = start
        self.backfill_days = backfill_days
        self.err429 = False
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password, database=database)


    def ch_insert(self, df, to_table):
        data_tuples = [tuple(x) for x in df.to_numpy()]
        self.client.insert(to_table, data_tuples, column_names=df.columns.tolist())




    def chunk_list(self, lst, chunk_size):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    def convert_to_timestamp(self, datetime_str):
        # Используем pandas для преобразования строки в timestamp
        timestamp = pd.to_datetime(datetime_str, utc=True).timestamp()
        # Округляем до целого числа для совместимости с ClickHouse DateTime
        return int(timestamp)

    def get_token(self, client_id, client_secret):
        try:
            host = 'https://api-performance.ozon.ru'
            endpoint = '/api/client/token'
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
            res = requests.post(host + endpoint, headers=headers, json=payload)
            if res.status_code == 200:
                message =  f"Платформа: OZON_ADS. Имя: {self.add_name}. Токен получен успешно"
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
            else:
                message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Ошибка получения токена: {res.status_code} {res.text}"
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
            response_json = res.json()
            if 'access_token' in response_json:
                access_token = response_json['access_token']
            else:
                access_token=None
            return access_token
        except Exception as e:
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Ошибка: {str(e)}"
            self.common.log_func(self.bot_token, self.chat_list, message, 3)


    def get_names(self, token):
        try:
            url = 'https://api-performance.ozon.ru:443/api/client/campaign'
            headers = {'Authorization': f'Bearer {token}','Content-Type': 'application/json','Accept': 'application/json'}
            response = requests.get(url, headers=headers)
            try:
                result = response.json()
            except:
                result = None
            message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_names. Код: {str(response.status_code)}'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            if response.status_code != 200:
                message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_names. Результат: {str(result)}'
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
            if response.status_code == 200 and result != None:
                campaigns = response.json()['list']
                df = pd.json_normalize(campaigns)
                pd.set_option('display.max_columns', None)
                df.rename(columns={'autopilot.maxBid': 'autopilot_maxBid'}, inplace=True)
                df.rename(columns={'autopilot.categoryId': 'autopilot_categoryId'}, inplace=True)
                df.rename(columns={'autopilot.filters': 'autopilot_filters'}, inplace=True)
                df.rename(columns={'autopilot.skuAddMode': 'autopilot_skuAddMode'}, inplace=True)
                df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')
                df['updatedAt'] = pd.to_datetime(df['updatedAt'], errors='coerce')
                df['timeStamp'] = self.now
                df['fromDate'] = pd.to_datetime(df['fromDate'], errors='coerce')
                df['toDate'] = pd.to_datetime(df['toDate'], errors='coerce')
                df['toDate'].fillna(pd.Timestamp.today().normalize(), inplace=True)
                df['fromDate'].fillna(pd.Timestamp.today().normalize(), inplace=True)
                df['id'] = df['id'].astype('int64')
                df['dailyBudget'] = df['dailyBudget'].astype('int64')
                df['autopilot_maxBid'] = df['autopilot_maxBid'].astype('int64')
                df['autopilot_categoryId'] = df['autopilot_categoryId'].astype('int64')
                df['weeklyBudget'] = df['weeklyBudget'].astype('int64')
                df['productAutopilotStrategy'] = df['productAutopilotStrategy'].astype(str)
                df['budget'] = df['budget'].astype('int64')
                df['placement'] = df['placement'].astype('str')
                df['autopilot_filters'] = df['autopilot_filters'].astype('str')
                required_columns = ['id', 'title', 'state', 'advObjectType', 'fromDate', 'toDate',
                        'dailyBudget', 'placement', 'budget', 'createdAt', 'updatedAt',
                        'productCampaignMode', 'productAutopilotStrategy', 'PaymentType',
                        'expenseStrategy', 'weeklyBudget', 'budgetType', 'startWeekDay',
                        'endWeekDay', 'autopilot_maxBid', 'autopilot_categoryId',
                        'autopilot_skuAddMode', 'autopilot_filters', 'timeStamp']
                df = df[required_columns]
                self.ch_insert(df, f"ozon_ads_campaigns_{self.add_name}")
                return response.status_code
        except Exception as e:
            message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_names. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def text_to_df(self, response_text, date):
        pd.set_option('display.max_columns', None)
        csv_file = response_text.splitlines()
        sp_columns = ['orderDate', 'orderId', 'orderNum', 'ozonId', 'productOzonId', 'artikul', 'name', 'count', 'price', 'value', 'rate', 'cost', ]
        sku_columns = ['sku', 'productName', 'productPrice', 'views', 'clicks', 'cost', 'orders', 'revenue',  'modelOrders', 'modelRevenue']
        banner_columns = ['banner', 'pageType', 'viewCond', 'platform', 'views', 'clicks', 'reach', 'cost']
        brand_shelf_columns = ['conditionType', 'viewCond', 'platform', 'views', 'clicks', 'reach', 'cost']
        sis_columns = ['pageType', 'views', 'clicks', 'cost', 'reach']

        replace_dict = {'Название товара': 'productName', 'Цена товара, ₽': 'productPrice', 'Расход, ₽, с НДС': 'cost',
                        'Показы': 'views', 'Заказы': 'orders', 'Клики': 'clicks', 'Выручка, ₽': 'revenue',
                        'Заказы модели': 'modelOrders', 'Выручка с заказов модели, ₽': 'modelRevenue',
                        'Тип страницы': 'pageType', 'Охват': 'reach', 'Тип условия': 'conditionType', 'Условие показа': 'viewCond',
                        'Платформа': 'platform', 'Баннер': 'banner', 'Дата': 'orderDate', 'ID заказа': 'orderId', 'Номер заказа': 'orderNum',
                        'Ozon ID': 'ozonId','Ozon ID продвигаемого товара': 'productOzonId', 'Артикул': 'artikul', 'Наименование': 'name',
                        'Количество': 'count', 'Цена продажи': 'price', 'Стоимость, ₽': 'value', 'Ставка, ₽': 'rate', 'Расход, ₽': 'cost'}

        int_list = ['views', 'clicks', 'orders', 'modelOrders', 'reach', 'count']
        float_list = ['cost', 'revenue', 'modelRevenue', 'productPrice', 'price', 'value', 'rate']
        str_list = ['orderNum', 'orderDate', 'productName', 'pageType', 'conditionType', 'viewCond', 'platform', 'banner', 'orderId', 'ozonId', 'productOzonId', 'artikul', 'name']


        add_to_table = "unknown"
        if len(csv_file)>1:
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
            df_filtered['timeStamp'] = self.now

            for col in int_list:
                try: df_filtered[col] = df_filtered[col].astype('int64')
                except: pass
            for col in float_list:
                try: df_filtered[col] = df_filtered[col].astype(str).str.replace(',', '.')
                except: pass
            for col in str_list:
                try: df_filtered[col] = df_filtered[col].astype(str)
                except: pass


            return [df_filtered, add_to_table]
        else:
            empty_df = pd.DataFrame()
            return [empty_df, add_to_table]

    def get_data(self, token, campaigns, date):
        if self.err429 == False:
            try:
                url = 'https://api-performance.ozon.ru:443/api/client/statistics'
                headers = {'Authorization': f'Bearer {token}'}
                payload = {
                    "campaigns": campaigns,
                    "dateFrom": date,
                    "dateTo": date,
                    "groupBy": "NO_GROUP_BY"
                }
                response = requests.post(url, headers=headers, json=payload)
                if response.status_code != 200:
                    message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Результат: {str(response.status_code)} {str(response.json())}'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                else:
                    report_uuid = response.json()['UUID']
                    url = f'https://api-performance.ozon.ru:443/api/client/statistics/{report_uuid}'
                    for k in range(200):
                        time.sleep(60)
                        try:
                            response = requests.get(url, headers=headers)
                            if response.json()['state']=="OK":
                                break
                        except:
                            pass
                    url = f'https://api-performance.ozon.ru:443/api/client/statistics/report?UUID={report_uuid}'
                    headers = {'Authorization': f'Bearer {token}'}
                    response = requests.get(url, headers=headers)
                    if len(campaigns) == 1:
                        text_df = self.text_to_df(response.text, date)
                        add_to_table = text_df[1]
                        df = text_df[0]
                        self.ch_insert(df, f"ozon_ads_data_{add_to_table}_{self.add_name}")
                    else:
                        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                            num = 0
                            for file_name in zip_file.namelist():
                                num +=1
                                with zip_file.open(file_name) as file:
                                    content = file.read().decode('utf-8')
                                    text_df = self.text_to_df(content, date)
                                    add_to_table = text_df[1]
                                    df = text_df[0]
                                    num_rows = df.shape[0]
                                    if num_rows>0:
                                        self.ch_insert(df, f"ozon_ads_data_{add_to_table}_{self.add_name}")
                                time.sleep(2)
                return response.status_code
            except Exception as e:
                message= f'Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Ошибка: {e}'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                return None
        else:
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(date)}. Функция: get_data. Ошибка 429, запрос не отправлен."
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def get_campaigns_in_period(self, token, start_date):
        try:
            end_date = self.yesterday.strftime("%Y-%m-%d")
            url = 'https://api-performance.ozon.ru:443/api/client/campaign'
            headers = {'Authorization': f'Bearer {token}','Content-Type': 'application/json','Accept': 'application/json'}
            response = requests.get(url, headers=headers)
            try:
                result = response.json()
            except:
                result = None
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Код: {str(response.status_code)}"
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            if response.status_code != 200:
                message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Ответ: {str(result)}"
                self.common.log_func(self.bot_token, self.chat_list, message, 1)
            if response.status_code == 200 and result != None:
                campaigns = response.json()['list']
                df = pd.json_normalize(campaigns)
                pd.set_option('display.max_columns', None)
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
            message = f"Платформа: OZON_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Ошибка: {e}"
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def create_date_list(self, start_date_str, end_date_str):
        try:
            # Преобразование строк в объекты datetime
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

            # Генерация списка дат
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
        optimize_data_sp = f"OPTIMIZE TABLE ozon_ads_data_sp_{self.add_name} FINAL"
        optimize_data_sku = f"OPTIMIZE TABLE ozon_ads_data_sku_{self.add_name} FINAL"
        optimize_data_banner = f"OPTIMIZE TABLE ozon_ads_data_banner_{self.add_name} FINAL"
        optimize_data_shelf = f"OPTIMIZE TABLE ozon_ads_data_shelf_{self.add_name} FINAL"
        optimize_data_sis = f"OPTIMIZE TABLE ozon_ads_data_sis_{self.add_name} FINAL"
        optimize_campaigns = f"OPTIMIZE TABLE ozon_ads_campaigns_{self.add_name} FINAL"
        optimize_collection = f"OPTIMIZE TABLE ozon_ads_collection_{self.add_name} FINAL"


        create_table_query_campaigns = f"""
        CREATE TABLE IF NOT EXISTS ozon_ads_campaigns_{self.add_name} (
            id UInt64, 
            title String, 
            state String, 
            advObjectType String, 
            fromDate Date, 
            toDate Date,
            dailyBudget Float, 
            placement String, 
            budget Float, 
            createdAt DateTime, 
            updatedAt DateTime,
            productCampaignMode String, 
            productAutopilotStrategy String, 
            PaymentType String,
            expenseStrategy String, 
            weeklyBudget Float, 
            budgetType String, 
            startWeekDay String,
            endWeekDay String, 
            autopilot_maxBid Float, 
            autopilot_categoryId UInt64,
            autopilot_skuAddMode String, 
            autopilot_filters String,
            timeStamp DateTime
        ) ENGINE = ReplacingMergeTree(timeStamp)
        ORDER BY id
        """

        create_table_query_data_sp = f"""
        CREATE TABLE IF NOT EXISTS ozon_ads_data_sp_{self.add_name} (
            date Date,
            id UInt64, 
            orderDate String,
            orderId String,
            orderNum String, 
            ozonId String, 
            productOzonId String, 
            artikul String, 
            name String,
            count UInt64, 
            price Float, 
            value Float, 
            rate Float, 
            cost Float,
            timeStamp DateTime
        ) ENGINE = ReplacingMergeTree(timeStamp)
        ORDER BY (date,id,orderDate,orderId,ozonId,productOzonId,artikul)
        """

        create_table_query_data_sku = f"""
        CREATE TABLE IF NOT EXISTS ozon_ads_data_sku_{self.add_name} (
            date Date,
            id UInt64, 
            sku String,
            productName String, 
            productPrice Float, 
            views UInt64, 
            clicks UInt64, 
            cost Float,
            orders UInt64, 
            revenue Float, 
            modelOrders UInt64, 
            modelRevenue Float, 
            timeStamp DateTime
        ) ENGINE = ReplacingMergeTree(timeStamp)
        ORDER BY (date,id,sku)
        """

        create_table_query_data_banner = f"""
            CREATE TABLE IF NOT EXISTS ozon_ads_data_banner_{self.add_name} (
                date Date,
                id UInt64, 
                banner String,
                pageType String, 
                viewCond String, 
                platform String, 
                views UInt64, 
                clicks UInt64, 
                cost Float,
                reach UInt64,  
                timeStamp DateTime
            ) ENGINE = ReplacingMergeTree(timeStamp)
            ORDER BY (date,id,banner,pageType,viewCond, platform)
            """

        create_table_query_data_shelf = f"""
                CREATE TABLE IF NOT EXISTS ozon_ads_data_shelf_{self.add_name} (
                    date Date,
                    id UInt64, 
                    conditionType String, 
                    viewCond String, 
                    platform String, 
                    views UInt64, 
                    clicks UInt64, 
                    cost Float,
                    reach UInt64,  
                    timeStamp DateTime
                ) ENGINE = ReplacingMergeTree(timeStamp)
                ORDER BY (date,id,conditionType,viewCond, platform)
            """

        create_table_query_data_sis = f"""
                CREATE TABLE IF NOT EXISTS ozon_ads_data_sis_{self.add_name} (
                    date Date,
                    id UInt64, 
                    pageType String, 
                    views UInt64, 
                    clicks UInt64, 
                    cost Float,
                    reach UInt64,  
                    timeStamp DateTime
                ) ENGINE = ReplacingMergeTree(timeStamp)
                ORDER BY (date,id,pageType)
            """

        create_table_query_collect = f"""
    CREATE TABLE IF NOT EXISTS ozon_ads_collection_{self.add_name} (
        date Date,
        campaignId UInt64,
        collect Bool
    ) ENGINE = ReplacingMergeTree(collect)
    ORDER BY (campaignId, date)
    """

        now = datetime.now()
        yesterday = now - timedelta(days=1)
        self.client.command(create_table_query_campaigns)
        self.client.command(create_table_query_data_sp)
        self.client.command(create_table_query_data_sis)
        self.client.command(create_table_query_data_sku)
        self.client.command(create_table_query_data_shelf)
        self.client.command(create_table_query_data_banner)
        self.client.command(create_table_query_collect)
        token = self.get_token(self.clientid, self.token)

        names_code = self.get_names(token)


        self.client.command(optimize_campaigns)
        time.sleep(5)

        if names_code == 200:
            active_campaigns = self.get_campaigns_in_period(token, self.start)

        # забираем активные из ozoncampaigns
            active_campaigns_query = f"""
                    SELECT id, createdAt, toDate
                    FROM ozon_ads_campaigns_{self.add_name}
                    WHERE id IN ({', '.join(map(str, active_campaigns))})
                    """
            active_campaigns_query_result = self.client.query(active_campaigns_query)
            df_campaigns = pd.DataFrame(active_campaigns_query_result.result_rows, columns=['id', 'createdAt', 'toDate'])
            df_campaigns['createdAt'] = pd.to_datetime(df_campaigns['createdAt']).dt.date
            df_campaigns['toDate'] = pd.to_datetime(df_campaigns['toDate']).dt.date

        # формируем список заданий для ozoncollection
            campaigns_date_list = []
            yesterday_date = yesterday.strftime("%Y-%m-%d")
            for _, row in df_campaigns.iterrows():
                advertId = row['id']
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

        # вставляем задания в ozoncollection и делаем оптимайз
            self.ch_insert(df_active_dates, f'ozon_ads_collection_{self.add_name}')
            self.client.command(optimize_collection)
            time.sleep(10)

        # отбираем несделанные даты из ozoncollection
            false_dates_query = f"""
                    SELECT distinct date  
                    FROM ozon_ads_collection_{self.add_name}
                    WHERE collect = False"""
            collect_days_rows = self.client.query(false_dates_query).result_rows
            collect_days = [item[0] for item in collect_days_rows]
            n_days_ago = now - timedelta(days=self.backfill_days)

        # для каждой даты находим актуальный список кампаний на сбор
            for day in collect_days:
                if self.err429 == False:
                    token = self.get_token(self.clientid, self.token)
                    difference = n_days_ago.date() - day
                    sql_date = day.strftime('%Y-%m-%d')
                    false_campaigns_by_date_query = f"""
                            SELECT campaignId  
                            FROM ozon_ads_collection_{self.add_name}
                            WHERE collect = False AND date = '{sql_date}'"""
                    campaigns_to_collect_rows = self.client.query(false_campaigns_by_date_query).result_rows
                    campaigns_to_collect =  list(set([str(item[0]) for item in campaigns_to_collect_rows]))

                    # делаем сбор по чанкам для каждой даты
                    for chunk in self.chunk_list(campaigns_to_collect, 10):
                        body = []
                        success_list = []
                        for campaign in chunk:
                            body.append(campaign)
                            if difference.days >= 0:
                                success_list.append((day, int(campaign), True))
                        message = f'Платформа: OZON_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Кампании: {str(body)}. Начало загрузки.'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)
            # получение данных и вставка в ozondata (единой транзакцией вместе с решением коллекшона)
                        try:
                            token = self.get_token(self.clientid, self.token)
                            ozon_json = self.get_data(token, body, sql_date)
                            if int(ozon_json)==429:
                                self.err429 = True
                            df_success = pd.DataFrame(success_list, columns=['date', 'campaignId', 'collect'])
                            if int(ozon_json)==200:
                                self.ch_insert(df_success, f'ozon_ads_collection_{self.add_name}')
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

        self.client.command(optimize_data_sp)
        time.sleep(5)
        self.client.command(optimize_data_banner)
        time.sleep(5)
        self.client.command(optimize_data_shelf)
        time.sleep(5)
        self.client.command(optimize_data_sku)
        time.sleep(10)
        self.client.command(optimize_data_sis)
        time.sleep(5)
        self.client.command(optimize_campaigns)
        time.sleep(10)

