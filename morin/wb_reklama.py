import requests
import time
import json
import os
import clickhouse_connect
import logging
import pandas as pd
from datetime import datetime, timedelta
from .common import Common

class WBreklama:
    def __init__(self, logging_path:str, subd: str, add_name: str, token: str , host: str, port: str, username: str, password: str, database: str, start: str, backfill_days: int):
        self.logging_path = logging_path
        self.token = token
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.subd = subd
        self.add_name = add_name.replace(' ','').replace('-','_')
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.yesterday = self.today - timedelta(days = 1)
        self.start = start
        self.common = Common(self.logging_path)
        self.backfill_days = backfill_days
        self.err429 = False
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password, database=database)
        logging.basicConfig(filename=self.logging_path,level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


    def get_names(self, campaign_list):
        try:
            headers = {"Authorization": self.token}
            url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
            response = requests.post(url, json=campaign_list, headers=headers)
            try:
                result = response.json()
            except:
                result = None
            logging.info("Код:" + str(response.status_code) + '  url:' + 'adv/v1/promotion/adverts (названия кампаний)')
            print("Код:" + str(response.status_code) + '  url:' + 'adv/v1/promotion/adverts (названия кампаний)')
            if response.status_code != 200:
                logging.info('Ответ: ' + str(result))
                print('Ответ: ' + str(result))
            if response.status_code == 429:
                self.err429 = True
            if response.status_code == 200 and result != None:
                df = pd.json_normalize(result)
                df['advertId'] = df['advertId'].astype('int64')
                required_columns = ["endTime", "createTime", "changeTime", "startTime", "name",  "dailyBudget", "advertId", "status", "type"]
                df = df[required_columns]
                pd.set_option('display.max_columns', None)
                data_types = df.dtypes.reset_index()
                df['endTime'] = df['endTime'].apply(self.convert_to_timestamp)
                df['createTime'] = df['createTime'].apply(self.convert_to_timestamp)
                df['changeTime'] = df['changeTime'].apply(self.convert_to_timestamp)
                df['startTime'] = df['startTime'].apply(self.convert_to_timestamp)
                df['timeStamp'] = self.now
                self.ch_insert(df, f"wb_ads_campaigns_{self.add_name}")
                return response.status_code
        except Exception as e:
            logging.info(f"Ошибка получения кампаний: {e}")
            print(f"Ошибка получения кампаний: {e}")
            return None

    def get_data(self, body, token):
        try:
            headers = {'Authorization': token, 'Content-Type': 'application/json'}
            url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
            json_data = json.dumps(body)
            response = requests.post(url, headers=headers, data=json_data)
            logging.info("Код:" + str(response.status_code) +'  url:' + 'adv/v2/fullstats')
            print("Код:" + str(response.status_code) + '  url:' + 'adv/v2/fullstats')
            try: result = response.json()
            except: result = None
            if response.status_code != 200:
                logging.info('Ответ: '+ str(result))
                print('Ответ: ' + str(result))
            if response.status_code == 429:
                self.err429 = True
            if response.status_code == 200 and result != None:
                final_df, final_booster_df = self.extract_df(result)
                self.ch_insert(final_df, f"wb_ads_data_{self.add_name}")
                self.ch_insert(final_booster_df, f"wb_ads_booster_{self.add_name}")
            return response.status_code
        except Exception as e:
            logging.info(f"Ошибка получения данных: {e}")
            print(f"Ошибка получения данных: {e}")
            return None

    def get_campaigns_in_period(self, campaign_list, token, start_date ):
        try:
            end_date = self.yesterday.strftime("%Y-%m-%d")
            headers = {"Authorization": token}
            url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
            response = requests.post(url, json=campaign_list, headers=headers)
            try:
                result = response.json()
            except:
                result = None
            logging.info("Код:" + str(response.status_code) + '  url:' + 'adv/v1/promotion/adverts (получение дат кампаний)')
            if response.status_code != 200:
                logging.info('Ответ: '+ str(result))
                print('Ответ: ' + str(result))
            if response.status_code == 200:
                df = pd.json_normalize(result)
                df['advertId'] = df['advertId'].astype('int64')
                required_columns = [  "advertId", "createTime", "endTime",]
                df = df[required_columns]
                pd.set_option('display.max_columns', None)
                df['endTime'] = df['endTime'].str[:10]
                df['createTime'] = df['createTime'].str[:10]
                df_filtered = df[((df['createTime'] <= end_date) & (df['endTime'] >= start_date))
                               | ((df['endTime'] >= start_date) & (df['createTime'] <= end_date))
                               | ((df['createTime'] >= start_date) & (df['endTime'] <= end_date))
                               | ((df['createTime'] <= start_date) & (df['endTime'] >= end_date))]
                advert_id_list = df_filtered['advertId'].tolist()

                return advert_id_list
            else:
                return None
        except Exception as e:
            logging.info(f"Ошибка получения кампаний для периода: {e}")
            print(f"Ошибка получения кампаний для периода: {e}")
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
            logging.info(f"Ошибка создания списка дат: {e}")
            print(f"Ошибка создания списка дат: {e}")
            return []

    def extract_df(self,in_json):
        out_json = []
        out_booster_json = []
        for advert in in_json:
            extract_advert = advert['advertId']
            try:
                booster_stats = advert['boosterStats']
                for booster in booster_stats:
                    try:
                        booster_date = booster['date'].replace('Z','')
                        booster_nm = booster['nm']
                        booster_avg = booster['avg_position']
                        out_booster_json.append({
                                            'advertId': extract_advert,
                                            'date': booster_date,
                                            'nm': booster_nm,
                                            'avgPosition': booster_avg     })
                    except:
                        pass
            except:
                pass
            for day in advert['days']:
                extract_date = day['date']
                for app in day['apps']:
                    extract_app = app['appType']
                    for nm in app['nm']:
                        extract_nm = nm['nmId']
                        try:
                            out_json.append({
                                'advertId': extract_advert,
                                'date': extract_date,
                                'appType': extract_app,
                                'nmId': extract_nm,
                                'views': nm['views'],
                                'clicks': nm['clicks'],
                                'sum': nm['sum'],
                                'atbs': nm['atbs'],
                                'orders': nm['orders'],
                                'shks': nm['shks'],
                                'sum_price': nm['sum_price'],
                                'name': nm['name']
                                })
                        except Exception as e:
                            logging.info(f"Строка nm: {nm}. Ошибка тут: {e}")
                            print(f"Строка nm: {nm}. Ошибка тут: {e}")
        df = pd.DataFrame(out_json)
        booster_df = pd.DataFrame(out_booster_json)
        df['date'] = pd.to_datetime(df['date']).dt.date
        booster_df['date'] = pd.to_datetime(booster_df['date']).dt.date
        pd.set_option('display.max_columns', None)
        df['timeStamp'] = self.now
        booster_df['timeStamp'] = self.now
        return df, booster_df

    def wb_reklama_collector(self):
        logging.basicConfig(
            filename=os.path.join(self.logging_path,'wb_ads_logs.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        create_table_query_campaigns = f"""
        CREATE TABLE IF NOT EXISTS wb_ads_campaigns_{self.add_name} (
            createTime DateTime,
            startTime DateTime,
            endTime DateTime,
            changeTime DateTime,
            timeStamp DateTime,
            name String,
            dailyBudget UInt64,
            advertId UInt64,
            status UInt64,
            type UInt64
        ) ENGINE = ReplacingMergeTree(timeStamp)
        ORDER BY advertId
        """

        create_table_query_data = f"""
        CREATE TABLE IF NOT EXISTS wb_ads_data_{self.add_name} (
            date Date,
            advertId UInt64,
            appType UInt64,
            nmId UInt64,
            name String,
            views UInt64,
            clicks UInt64,
            sum Float64,
            atbs UInt64,
            orders UInt64,
            shks UInt64,
            sum_price Float64,
            timeStamp DateTime
        ) ENGINE = ReplacingMergeTree(timeStamp)
        ORDER BY (advertId, date, appType, nmId)
        PARTITION BY date
        """

        create_table_query_booster = f"""
                CREATE TABLE IF NOT EXISTS wb_ads_booster_{self.add_name} (
                    date Date,
                    advertId UInt64,
                    nm UInt64,
                    avgPosition UInt64,
                    timeStamp DateTime
                ) ENGINE = ReplacingMergeTree(timeStamp)
                ORDER BY (advertId, nm, date)
                PARTITION BY date
                """

        create_table_query_collect = f"""
        CREATE TABLE IF NOT EXISTS wb_ads_collection_{self.add_name} (
            date Date,
            advertId UInt64,
            collect Bool
        ) ENGINE = ReplacingMergeTree(collect)
        ORDER BY (advertId, date)
        """

        optimize_data = f"OPTIMIZE TABLE wb_ads_data_{self.add_name} FINAL"
        optimize_booster = f"OPTIMIZE TABLE wb_ads_booster_{self.add_name} FINAL"
        optimize_campaigns = f"OPTIMIZE TABLE wb_ads_campaigns_{self.add_name} FINAL"
        optimize_collection = f"OPTIMIZE TABLE wb_ads_collection_{self.add_name} FINAL"

        now = datetime.now()
        yesterday = now - timedelta(days=1)
        self.client.command(create_table_query_campaigns)
        self.client.command(create_table_query_data)
        self.client.command(create_table_query_booster)
        self.client.command(create_table_query_collect)
        headers = {"Authorization": self.token}
        url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logging.info(f"Ошибка получения списка: {response.status_code}")
            print(f"Ошибка получения списка: {response.status_code}")
        else:
            result = response.json()
            advert_ids = []
            for advert in result['adverts']:
                for item in advert['advert_list']:
                    advert_ids.append(item['advertId'])
            for chunk in self.chunk_list(advert_ids, 50):
                self.get_names(chunk)
                time.sleep(10)

            active_campaigns = []
            for chunk in self.chunk_list(advert_ids, 50):
                active_campaigns=active_campaigns + self.get_campaigns_in_period(chunk, self.token, self.start)
                time.sleep(10)
            logging.info("Активные кампании: "+ str(active_campaigns))
            print("Активные кампании: " + str(active_campaigns))

        # забираем активные из wbcampaigns
            active_campaigns_query = f"""
                    SELECT advertId, createTime, endTime
                    FROM wb_ads_campaigns_{self.add_name}
                    WHERE advertId IN ({', '.join(map(str, active_campaigns))})
                    """
            active_campaigns_query_result = self.client.query(active_campaigns_query)
            df_campaigns = pd.DataFrame(active_campaigns_query_result.result_rows, columns=['advertId', 'createTime', 'endTime'])
            df_campaigns['createTime'] = pd.to_datetime(df_campaigns['createTime']).dt.date
            df_campaigns['endTime'] = pd.to_datetime(df_campaigns['endTime']).dt.date

        # формируем список заданий для wbcollection
            campaigns_date_list = []
            yesterday_date = yesterday.strftime("%Y-%m-%d")
            for _, row in df_campaigns.iterrows():
                advertId = row['advertId']
                start_date = row['createTime'].strftime('%Y-%m-%d')
                end_date = row['endTime'].strftime('%Y-%m-%d')
                if end_date > yesterday_date:
                    end_date = yesterday_date
                if start_date < self.start:
                    start_date = self.start
                date_list = self.create_date_list(start_date, end_date)
                for date in date_list:
                    campaigns_date_list.append((datetime.strptime(date, '%Y-%m-%d').date(), advertId, False))
            df_active_dates = pd.DataFrame(campaigns_date_list, columns=['date', 'advertId', 'collect'])


        # вставляем задания в wbcollection и делаем оптимайз
            self.ch_insert(df_active_dates, f'wb_ads_collection_{self.add_name}')
            time.sleep(20)

            self.client.command(optimize_collection)
            time.sleep(20)

        # отбираем несделанные даты из wbcollection
            false_dates_query = f"""
                    SELECT distinct date  
                    FROM wb_ads_collection_{self.add_name}
                    WHERE collect = False"""
            collect_days_rows = self.client.query(false_dates_query).result_rows
            collect_days = [item[0] for item in collect_days_rows]
            n_days_ago = now - timedelta(days=self.backfill_days)


        # для каждой даты находим актуальный список кампаний на сбор
            for day in collect_days:
                if self.err429 == False:
                    difference = n_days_ago.date() - day
                    sql_date = day.strftime('%Y-%m-%d')
                    false_campaigns_by_date_query = f"""
                            SELECT advertId  
                            FROM wb_ads_collection_{self.add_name}
                            WHERE collect = False AND date = '{sql_date}'"""
                    campaigns_to_collect_rows = self.client.query(false_campaigns_by_date_query).result_rows
                    campaigns_to_collect =  list(set([item[0] for item in campaigns_to_collect_rows]))

            # делаем сбор по чанкам для каждой даты
                    for chunk in self.chunk_list(campaigns_to_collect, 50):
                        body = []
                        success_list = []
                        for campaign in chunk:
                            body.append({"id": int(campaign), "dates": [sql_date]})
                            if difference.days >= 0:
                                success_list.append((day, campaign, True))
                        logging.info("Получаем дату: " + str(sql_date) + '  Кампании: ' +str(chunk))
                        print("Получаем дату: " + str(sql_date) + '  Кампании: ' +str(chunk))

            # получение данных и вставка в wbdata (единой транзакцией вместе с решением коллекшона)
                        try:
                            wb_json = self.get_data(body, self.token)
                            df_success = pd.DataFrame(success_list, columns=['date', 'advertId', 'collect'])
                            if int(wb_json)==200:
                                self.ch_insert(df_success, f'wb_ads_collection_{self.add_name}')
                                logging.info("День загружен: " + str(sql_date) + "  Кампании: " + str(chunk))
                                print("День загружен: " + str(sql_date) + "  Кампании: " + str(chunk))
                                self.client.command(optimize_collection)
                        except Exception as e:
                            logging.info("Ошибка: " +str(e))
                            print("Ошибка: " + str(e))
                        time.sleep(61)

        self.client.command(optimize_data)
        time.sleep(20)
        self.client.command(optimize_booster)
        time.sleep(10)
        self.client.command(optimize_campaigns)
        time.sleep(20)
        self.common.keep_last_20000_lines(self.logging_path)
