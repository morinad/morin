import time
import os
import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
from .common import Common
from .clickhouse import Clickhouse
from .base_client import BaseMarketplaceClient

class WBreklama:
    def __init__(self, bot_token:str, chat_list:str, message_type: str, subd: str, add_name: str, token: str , host: str, port: str, username: str, password: str, database: str, start: str, backfill_days: int):
        self.bot_token = bot_token
        self.chat_list = chat_list
        self.message_type = message_type
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
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.backfill_days = backfill_days
        self.err429 = False
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password, database=database)
        self.clickhouse = Clickhouse(bot_token, chat_list, message_type, host, port, username, password, database, start, self.add_name, self.err429, backfill_days, 'wb_ads')
        self.api = BaseMarketplaceClient(
            base_url='https://advert-api.wildberries.ru',
            headers={'Authorization': self.token},
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )

    def chunk_list(self, lst, chunk_size):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]


    def get_names(self, campaign_list):
        try:
            ids_str = ','.join(str(c) for c in campaign_list)
            result = self.api._request('GET', '/api/advert/v2/adverts', params={'ids': ids_str})
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: get_names. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            campaigns = result.get('adverts', [])
            rows = []
            yesterday_str = self.yesterday.strftime('%Y-%m-%dT00:00:00+03:00')
            for c in campaigns:
                ts = c.get('timestamps', {})
                status = c.get('status', 0)
                if status == -1:
                    end_time = (ts.get('deleted') or ts.get('updated') or '')
                elif status in (7, 8):
                    end_time = (ts.get('updated') or '')
                else:
                    end_time = yesterday_str
                rows.append({
                    'endTime': end_time,
                    'createTime': (ts.get('created') or ''),
                    'changeTime': (ts.get('updated') or ''),
                    'startTime': (ts.get('started') or ts.get('created') or ''),
                    'name': c.get('settings', {}).get('name', ''),
                    'dailyBudget': 0,
                    'advertId': c['id'],
                    'status': status,
                    'type': 9,
                })
            table_name = f"wb_ads_campaigns_{self.add_name}"
            text_columns_set = self.clickhouse.ch_text_columns_set(table_name)
            self.clickhouse.create_alter_ch(rows, table_name, 'advertId', '', 'ReplacingMergeTree(timeStamp)')
            df = self.common.check_and_convert_types(rows, 'advertId', '', text_columns_set)
            self.clickhouse.ch_insert(df, table_name)
            return 200
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: get_names. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def get_data(self, body):
        try:
            ids_str = ','.join(str(item['id']) for item in body)
            date_str = body[0]['dates'][0]
            params = {'ids': ids_str, 'beginDate': date_str, 'endDate': date_str}
            result = self.api._request('GET', '/adv/v3/fullstats', params=params)
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: get_data. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            final_df, final_booster_df, out_json, out_booster_json = self.extract_df(result)
            data_table = f"wb_ads_data_{self.add_name}"
            booster_table = f"wb_ads_booster_{self.add_name}"
            if len(out_json) > 0:
                text_columns_set = self.clickhouse.ch_text_columns_set(data_table)
                self.clickhouse.create_alter_ch(out_json, data_table, 'advertId,date,appType,nmId', 'date', 'ReplacingMergeTree(timeStamp)')
                df = self.common.check_and_convert_types(out_json, 'advertId,date,appType,nmId', 'date', text_columns_set)
                self.clickhouse.ch_insert(df, data_table)
            if len(out_booster_json) > 0:
                text_columns_set_b = self.clickhouse.ch_text_columns_set(booster_table)
                self.clickhouse.create_alter_ch(out_booster_json, booster_table, 'advertId,nm,date', 'date', 'ReplacingMergeTree(timeStamp)')
                booster_df = self.common.check_and_convert_types(out_booster_json, 'advertId,nm,date', 'date', text_columns_set_b)
                self.clickhouse.ch_insert(booster_df, booster_table)
            return 200
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: get_data. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None

    def get_campaigns_in_period(self, campaign_list, start_date):
        try:
            end_date = self.yesterday.strftime("%Y-%m-%d")
            ids_str = ','.join(str(c) for c in campaign_list)
            result = self.api._request('GET', '/api/advert/v2/adverts', params={'ids': ids_str})
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            if result is not None:
                campaigns = result.get('adverts', [])
                rows = []
                yesterday_str = end_date
                for c in campaigns:
                    ts = c.get('timestamps', {})
                    status = c.get('status', 0)
                    created = (ts.get('created') or '')[:10]
                    if status == -1:
                        et = (ts.get('deleted') or ts.get('updated') or '')[:10]
                    elif status in (7, 8):
                        et = (ts.get('updated') or '')[:10]
                    else:
                        et = yesterday_str
                    rows.append({
                        'advertId': c['id'],
                        'createTime': created,
                        'endTime': et,
                    })
                if not rows:
                    return []
                df = pd.DataFrame(rows)
                df['advertId'] = df['advertId'].astype('int64')
                df_filtered = df[((df['createTime'] <= end_date) & (df['endTime'] >= start_date))
                               | ((df['endTime'] >= start_date) & (df['createTime'] <= end_date))
                               | ((df['createTime'] >= start_date) & (df['endTime'] <= end_date))
                               | ((df['createTime'] <= start_date) & (df['endTime'] >= end_date))]
                advert_id_list = df_filtered['advertId'].tolist()
                return advert_id_list
            else:
                return []
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: get_campaigns_in_period. Ошибка: {e}'
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
            message =f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: create_date_list. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return []

    def extract_df(self,in_json):
        try:
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
                    try:
                        extract_date = day['date']
                        for app in day['apps']:
                            extract_app = app['appType']
                            for nm in app['nms']:
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
                                    message = f"Строка nm: {nm}. Не найдено: {e}"
                                    self.common.log_func(self.bot_token, self.chat_list, message, 1)
                    except Exception as e:
                        message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: extract_df. Ошибка распознавания {e}: {str(day)[:1000]}'
                        self.common.log_func(self.bot_token, self.chat_list, message, 3)
            pd.set_option('display.max_columns', None)
            df = pd.DataFrame(out_json)
            booster_df = pd.DataFrame(out_booster_json)
            df['date'] = pd.to_datetime(df['date']).dt.date
            if len(out_booster_json)>0:
                booster_df['date'] = pd.to_datetime(booster_df['date']).dt.date
                booster_df['timeStamp'] = self.now
            df['timeStamp'] = self.now
        except Exception as e:
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: extract_df. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
        return df, booster_df, out_json, out_booster_json

    def wb_reklama_collector(self):
        create_table_query_collect = f"""
        CREATE TABLE IF NOT EXISTS wb_ads_collection_{self.add_name} (
            date Date,
            advertId UInt64,
            collect Bool
        ) ENGINE = ReplacingMergeTree(collect)
        ORDER BY (advertId, date)
        """

        optimize_collection = f"OPTIMIZE TABLE wb_ads_collection_{self.add_name} FINAL"

        now = datetime.now()
        yesterday = now - timedelta(days=1)
        self.client.command(create_table_query_collect)
        try:
            result = self.api._request('GET', '/adv/v1/promotion/count')
            advert_ids = []
            for advert in result['adverts']:
                for item in advert['advert_list']:
                    advert_ids.append(item['advertId'])
            for chunk in self.chunk_list(advert_ids, 50):
                self.get_names(chunk)
                time.sleep(10)

            active_campaigns = []
            for chunk in self.chunk_list(advert_ids, 50):
                chunk_campaigns = self.get_campaigns_in_period(chunk, self.start)
                if chunk_campaigns:
                    active_campaigns = active_campaigns + chunk_campaigns
                time.sleep(10)
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Активные кампании: {str(active_campaigns)}'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)

            active_campaigns_query = f"""
                    SELECT advertId, createTime, endTime
                    FROM wb_ads_campaigns_{self.add_name}
                    WHERE advertId IN ({', '.join(map(str, active_campaigns))})
                    """
            active_campaigns_query_result = self.client.query(active_campaigns_query)
            df_campaigns = pd.DataFrame(active_campaigns_query_result.result_rows, columns=['advertId', 'createTime', 'endTime'])
            df_campaigns['createTime'] = pd.to_datetime(df_campaigns['createTime']).dt.date
            df_campaigns['endTime'] = pd.to_datetime(df_campaigns['endTime']).dt.date

            campaigns_date_list = []
            yesterday_date = yesterday.strftime("%Y-%m-%d")
            for _, row in df_campaigns.iterrows():
                advertId = int(row['advertId'])
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

            self.client.insert(f'wb_ads_collection_{self.add_name}', [tuple(x) for x in df_active_dates.to_numpy()], column_names=df_active_dates.columns.tolist())
            time.sleep(20)

            self.client.command(optimize_collection)
            time.sleep(20)

            false_dates_query = f"""
                    SELECT distinct date
                    FROM wb_ads_collection_{self.add_name}
                    WHERE collect = False"""
            collect_days_rows = self.client.query(false_dates_query).result_rows
            collect_days = [item[0] for item in collect_days_rows]
            n_days_ago = now - timedelta(days=self.backfill_days)

            for day in collect_days:
                if self.err429 == False:
                    difference = n_days_ago.date() - day
                    sql_date = day.strftime('%Y-%m-%d')
                    false_campaigns_by_date_query = f"""
                            SELECT advertId
                            FROM wb_ads_collection_{self.add_name}
                            WHERE collect = False AND date = '{sql_date}'"""
                    campaigns_to_collect_rows = self.client.query(false_campaigns_by_date_query).result_rows
                    campaigns_to_collect = list(set([item[0] for item in campaigns_to_collect_rows]))

                    for chunk in self.chunk_list(campaigns_to_collect, 50):
                        body = []
                        success_list = []
                        for campaign in chunk:
                            body.append({"id": int(campaign), "dates": [sql_date]})
                            if difference.days >= 0:
                                success_list.append((day, campaign, True))
                        message = f'Платформа: WB_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Кампании: {str(chunk)}'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)

                        try:
                            wb_json = self.get_data(body)
                            df_success = pd.DataFrame(success_list, columns=['date', 'advertId', 'collect'])
                            if wb_json is not None and int(wb_json) == 200:
                                self.client.insert(f'wb_ads_collection_{self.add_name}', [tuple(x) for x in df_success.to_numpy()], column_names=df_success.columns.tolist())
                                message = f'Платформа: WB_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Кампании: {str(chunk)}. Результат: ОК'
                                self.common.log_func(self.bot_token, self.chat_list, message, 2)
                                self.client.command(optimize_collection)
                        except Exception as e:
                            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Дата: {str(sql_date)}. Ошибка: {str(e)}'
                            self.common.log_func(self.bot_token, self.chat_list, message, 3)
                        time.sleep(90)
        except Exception as e:
            if hasattr(self, 'api') and self.api.err429:
                self.err429 = True
            message = f'Платформа: WB_ADS. Имя: {self.add_name}. Функция: wb_reklama. Ошибка: {e}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)



        self.client.command(optimize_collection)
        time.sleep(10)

