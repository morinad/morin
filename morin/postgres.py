from .common import Common
from datetime import datetime, timedelta
import pandas as pd
import time
from io import StringIO

try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PG = True
except ImportError:
    HAS_PG = False


CH_TO_PG_TYPES = {
    'Float64': 'DOUBLE PRECISION',
    'String': 'TEXT',
    'Date': 'DATE',
    'DateTime': 'TIMESTAMP',
    'UInt8': 'BOOLEAN',
}


def _q(name):
    return '"' + str(name).replace('"', '""') + '"'


def _ch_to_pg(ch_type):
    ch_type = ch_type.strip()
    return CH_TO_PG_TYPES.get(ch_type, 'TEXT')


class Postgres:
    def __init__(self, bot_token: str, chat_list: str, message_type: str, host: str, port: str, username: str, password: str, database: str, start: str, add_name: str, err429: bool, backfill_days: int, platform: str):
        if not HAS_PG:
            raise ImportError("psycopg2 не установлен. Установите: pip install psycopg2-binary")
        self.bot_token = bot_token
        self.chat_list = chat_list
        self.message_type = message_type
        self.host = host
        self.port = port if port else 5432
        self.username = username
        self.password = password
        self.database = database
        self.now = datetime.now()
        self.start = start
        self.add_name = add_name
        self.err429 = err429
        self.backfill_days = backfill_days
        self.today = datetime.now().date()
        self.platform = platform
        self.common = Common(self.bot_token, self.chat_list, self.message_type)

    def _conn(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.database,
        )

    def test_clickhouse_connection(self):
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.fetchone()
            cur.close()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Подключение к PostgreSQL успешно!'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return True
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Ошибка подключения к PostgreSQL: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return False
        finally:
            if conn:
                conn.close()

    def ch_execute(self, expression):
        conn = None
        disp_exp = expression.strip()[:60] + '...'
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(expression)
            conn.commit()
            cur.close()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Выражение {disp_exp} выполнено.'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            if conn:
                conn.rollback()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Ошибка выражения {disp_exp}: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
        finally:
            if conn:
                conn.close()

    def ch_check(self, table_name):
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
                (table_name,)
            )
            row = cur.fetchone()
            cur.close()
            if row:
                print(f'Таблица {table_name} существует.')
                return True
            else:
                print(f'Таблица {table_name} не существует.')
                return False
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return False
        finally:
            if conn:
                conn.close()

    def ch_text_columns_set(self, table_name):
        conn = None
        text_columns_set = set()
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
                (table_name,)
            )
            rows = cur.fetchall()
            cur.close()
            for col_name, data_type in rows:
                if data_type and 'text' in data_type.lower():
                    text_columns_set.add(col_name.strip())
        except:
            pass
        finally:
            if conn:
                conn.close()
        return text_columns_set

    def _existing_columns(self, conn, table_name):
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
            (table_name,)
        )
        rows = cur.fetchall()
        cur.close()
        return {col_name: data_type for col_name, data_type in rows}

    def create_alter_ch(self, data, table_name, uniq_columns, partitions, mergetree):
        conn = None
        try:
            print(table_name)
            text_columns_set = self.ch_text_columns_set(table_name)
            upload_list = self.common.analyze_column_types(data, uniq_columns, partitions, text_columns_set)
            print('upload_list', upload_list)
            cleaned = []
            for item in upload_list:
                if 'None' in item:
                    continue
                parts = item.split(' ', 1)
                if len(parts) != 2:
                    continue
                col, ch_type = parts[0].strip(), parts[1].strip()
                pg_type = _ch_to_pg(ch_type)
                cleaned.append((col, pg_type))
            cleaned.append(('timeStamp', 'TIMESTAMP'))

            conn = self._conn()
            cur = conn.cursor()

            cols_ddl = ', '.join(f'{_q(c)} {t}' for c, t in cleaned)
            create_sql = f'CREATE TABLE IF NOT EXISTS {_q(table_name)} ({cols_ddl})'
            cur.execute(create_sql)

            uniq_list = [c.strip() for c in uniq_columns.split(',') if c.strip()]
            if uniq_list and 'ReplacingMergeTree' in (mergetree or ''):
                idx_name = f'{table_name}_uniq_idx'
                idx_cols = ', '.join(_q(c) for c in uniq_list)
                cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {_q(idx_name)} ON {_q(table_name)} ({idx_cols})')

            if partitions and partitions.strip():
                idx_name = f'{table_name}_part_idx'
                cur.execute(f'CREATE INDEX IF NOT EXISTS {_q(idx_name)} ON {_q(table_name)} ({_q(partitions.strip())})')

            existing = self._existing_columns(conn, table_name)
            for col, pg_type in cleaned:
                if col not in existing:
                    alter_sql = f'ALTER TABLE {_q(table_name)} ADD COLUMN IF NOT EXISTS {_q(col)} {pg_type}'
                    message = f'Платформа: {self.platform}. Имя: {self.add_name}. Попытка изменения {table_name}. Формула: {alter_sql}'
                    self.common.log_func(self.bot_token, self.chat_list, message, 1)
                    cur.execute(alter_sql)
                    message = f'Платформа: {self.platform}. Имя: {self.add_name}. Успешное изменение {table_name}. Формула: {alter_sql}'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                    time.sleep(0.2)

            conn.commit()
            cur.close()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Данные готовы для вставки в {table_name}'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            if conn:
                conn.rollback()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Функция: create_alter_ch. Ошибка подготовки данных: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
        finally:
            if conn:
                conn.close()

    def ch_insert(self, df, to_table, chunk_size=5000, merge_type=None, uniq_columns=None):
        conn = None
        try:
            if df is None or len(df) == 0:
                return
            cols = df.columns.tolist()
            cols_quoted = ', '.join(_q(c) for c in cols)

            on_conflict = ''
            if merge_type and 'ReplacingMergeTree' in merge_type and uniq_columns:
                uniq_list = [c.strip() for c in uniq_columns.split(',') if c.strip()]
                if uniq_list:
                    update_cols = [c for c in cols if c not in uniq_list]
                    if update_cols:
                        set_clause = ', '.join(f'{_q(c)} = EXCLUDED.{_q(c)}' for c in update_cols)
                        conflict_cols = ', '.join(_q(c) for c in uniq_list)
                        if 'timeStamp' in cols:
                            on_conflict = (
                                f' ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause} '
                                f'WHERE EXCLUDED.{_q("timeStamp")} >= {_q(to_table)}.{_q("timeStamp")}'
                            )
                        else:
                            on_conflict = f' ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}'
                    else:
                        conflict_cols = ', '.join(_q(c) for c in uniq_list)
                        on_conflict = f' ON CONFLICT ({conflict_cols}) DO NOTHING'

            sql = f'INSERT INTO {_q(to_table)} ({cols_quoted}) VALUES %s{on_conflict}'

            conn = self._conn()
            cur = conn.cursor()

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                tuples = [tuple(self._py_value(v) for v in row) for row in chunk.to_numpy()]
                execute_values(cur, sql, tuples, page_size=1000)
                conn.commit()
                time.sleep(0.2)

            cur.close()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {to_table}. Результат: данные вставлены в PG!'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
        except Exception as e:
            if conn:
                conn.rollback()
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {to_table}. Функция: ch_insert. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            raise
        finally:
            if conn:
                conn.close()

    def _py_value(self, v):
        if v is None:
            return None
        if isinstance(v, float):
            try:
                if v != v:
                    return 0.0
            except Exception:
                pass
            return v
        if hasattr(v, 'item'):
            try:
                return v.item()
            except Exception:
                pass
        if isinstance(v, pd.Timestamp):
            try:
                return v.to_pydatetime()
            except Exception:
                pass
        return v

    def get_missing_dates(self, table_name, report_name, start_date_str, include_today):
        conn = None
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                f'SELECT date FROM {_q(table_name)} WHERE report = %s AND collect = TRUE',
                (report_name,)
            )
            existing_dates = {row[0] for row in cur.fetchall()}
            cur.close()
            current_date = start_date
            all_dates = set()
            if include_today:
                while current_date <= self.today:
                    all_dates.add(current_date)
                    current_date += timedelta(days=1)
            else:
                while current_date < self.today:
                    all_dates.add(current_date)
                    current_date += timedelta(days=1)
            missing_dates = sorted(all_dates - existing_dates)
            missing_dates_str = [d.strftime('%Y-%m-%d') for d in missing_dates]
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Старт: {start_date}. Функция: get_missing_dates. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return missing_dates_str
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Функция: get_missing_dates. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None
        finally:
            if conn:
                conn.close()

    def get_table_data(self, table_name, columns, condition=None):
        conn = None
        try:
            if isinstance(columns, list):
                columns_str = ', '.join(_q(c) for c in columns)
                col_names = list(columns)
            else:
                columns_str = columns
                col_names = None
            where = f'WHERE {condition}' if condition else ''
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(f'SELECT {columns_str} FROM {_q(table_name)} {where}')
            rows = cur.fetchall()
            if col_names is None:
                col_names = [d[0] for d in cur.description]
            cur.close()
            existing_values = [dict(zip(col_names, row)) for row in rows]
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Функция: get_table_data. Результат: ОК'
            self.common.log_func(self.bot_token, self.chat_list, message, 1)
            return existing_values
        except Exception as e:
            message = f'Платформа: {self.platform}. Имя: {self.add_name}. Таблица: {table_name}. Функция: get_table_data. Ошибка: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            return None
        finally:
            if conn:
                conn.close()

    def upload_data(self, platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date):
        try:
            if self.err429 == False:
                n_days_ago = self.today - timedelta(days=self.backfill_days)
                table_name = f'{platform}_{upload_table}_{self.add_name}'

                text_columns_set = self.ch_text_columns_set(table_name)

                if refresh_type == 'delete_date' and partitions and partitions.strip():
                    refresh = f"DELETE FROM {_q(table_name)} WHERE {_q(partitions.strip())} = '{date}';"
                elif refresh_type == 'delete_all':
                    refresh = f"TRUNCATE TABLE {_q(table_name)};"
                else:
                    refresh = None
                print(refresh)
                data = func_name(date)
                if not self.common.is_error(data):
                    collect = True
                    if history and datetime.strptime(date, '%Y-%m-%d').date() >= n_days_ago:
                        collect = False
                    collection_data = pd.DataFrame({
                        'date': [datetime.strptime(date, '%Y-%m-%d').date()],
                        'report': [report_name],
                        'collect': [collect]
                    })
                    if self.common.is_empty(data):
                        message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Дата: {date}. ПУСТОЙ ОТВЕТ!'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)
                    if not self.common.is_empty(data):
                        self.create_alter_ch(data, table_name, uniq_columns, partitions, merge_type)
                        df = self.common.check_and_convert_types(data, uniq_columns, partitions, text_columns_set)
                    if self.ch_check(table_name) and refresh:
                        self.ch_execute(refresh)
                    if not self.common.is_empty(data):
                        self.ch_insert(df, table_name, merge_type=merge_type, uniq_columns=uniq_columns)
                    self.ch_insert(collection_data, f'{platform}_collection_{self.add_name}', merge_type='ReplacingMergeTree(collect)', uniq_columns='report,date')
                    message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Дата: {date}. Данные добавлены!'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                time.sleep(delay)
            else:
                message = f'Платформа: {platform}. Имя: {self.add_name}. Таблица: {report_name}. Функция: upload_data. Ошибка: 429.'
                self.common.log_func(self.bot_token, self.chat_list, message, 3)
                raise ValueError("Обнаружена ошибка 429")
        except Exception as e:
            message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Дата: {date}. Ошибка вставки: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
            time.sleep(delay)

    def collecting_report(self, platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, frequency, delay):
        try:
            self.test_clickhouse_connection()
            collection_table = f'{platform}_collection_{self.add_name}'
            create_table_query_collect = (
                f'CREATE TABLE IF NOT EXISTS {_q(collection_table)} '
                f'(date DATE, report TEXT, collect BOOLEAN)'
            )
            create_uniq_idx = (
                f'CREATE UNIQUE INDEX IF NOT EXISTS {_q(collection_table + "_uniq_idx")} '
                f'ON {_q(collection_table)} (report, date)'
            )
            self.ch_execute(create_table_query_collect)
            self.ch_execute(create_uniq_idx)
            time.sleep(1)
            if history:
                date_list = self.get_missing_dates(collection_table, report_name, self.start, False)
                if date_list is None:
                    return
                for date in date_list:
                    if self.err429 == False and self.common.to_collect(frequency, date):
                        message = f'Платформа: {platform}. Имя: {self.add_name}. Таблица: {upload_table}. Репорт: {report_name}. Дата: {date}. Начинаем сбор...'
                        self.common.log_func(self.bot_token, self.chat_list, message, 2)
                        self.upload_data(platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date)
            else:
                date = self.today.strftime('%Y-%m-%d')
                if self.err429 == False and self.common.to_collect(frequency, date):
                    message = f'Платформа: {platform}. Имя: {self.add_name}. Таблица: {upload_table}. Репорт: {report_name}. Дата: {date}. Начинаем сбор...'
                    self.common.log_func(self.bot_token, self.chat_list, message, 2)
                    self.upload_data(platform, report_name, upload_table, func_name, uniq_columns, partitions, merge_type, refresh_type, history, delay, date)
        except Exception as e:
            message = f'Платформа: {platform}. Имя: {self.add_name}. Репорт: {report_name}. Функция: collecting_report. Ошибка сбора: {e}'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)
