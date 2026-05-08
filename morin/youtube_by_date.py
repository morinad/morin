from .common import Common
from .db import make_db
from .base_client import BaseMarketplaceClient
import httpx
from datetime import datetime, timedelta
import time
import csv as csv_module
from io import StringIO


class YOUTUBEbyDate:
    YOUTUBE_CLIENT_ID = '468588465628-rb54ab06irarfvf97jnkit1egd63hog1.apps.googleusercontent.com'
    YOUTUBE_CLIENT_SECRET = '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'

    def __init__(self, bot_token: str = '', chats: str = '', message_type: str = '', subd: str = '',
                 host: str = '', port: str = '', username: str = '', password: str = '', database: str = '',
                 add_name: str = '', refresh_token: str = '',
                 start: str = '', backfill_days: int = 0, reports: str = ''):
        self.bot_token = bot_token
        self.chat_list = chats.replace(' ', '').split(',')
        self.message_type = message_type
        self.common = Common(self.bot_token, self.chat_list, self.message_type)
        self.client_id = self.YOUTUBE_CLIENT_ID
        self.client_secret = self.YOUTUBE_CLIENT_SECRET
        self.refresh_token = refresh_token
        self.subd = subd
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.add_name = self.common.transliterate_key(add_name)
        self.now = datetime.now()
        self.today = datetime.now().date()
        self.start = start
        self.reports = reports
        self.backfill_days = backfill_days
        self.platform = 'youtube'
        self.err429 = False
        self.access_token = None
        self.token_acquired_at = None
        self._cached_channel_id = None
        self._cached_video_ids = None

        self.api = BaseMarketplaceClient(
            base_url='https://www.googleapis.com',
            headers={'Content-Type': 'application/json'},
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )
        self.analytics_api = BaseMarketplaceClient(
            base_url='https://youtubeanalytics.googleapis.com',
            headers={'Content-Type': 'application/json'},
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )
        self.reporting_api = BaseMarketplaceClient(
            base_url='https://youtubereporting.googleapis.com',
            headers={'Content-Type': 'application/json'},
            bot_token=self.bot_token,
            chat_list=self.chat_list,
            common=self.common,
            name=self.add_name
        )

        self.source_dict = {
            'videos': {
                'platform': 'youtube',
                'report_name': 'videos',
                'upload_table': 'videos',
                'func_name': self.get_videos,
                'uniq_columns': 'id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 30
            },
            'analytics': {
                'platform': 'youtube',
                'report_name': 'analytics',
                'upload_table': 'analytics',
                'func_name': self.get_analytics,
                'uniq_columns': 'day,video',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 30
            },
            'analytics_full': {
                'platform': 'youtube',
                'report_name': 'analytics_full',
                'upload_table': 'analytics_full',
                'func_name': self.get_analytics_full,
                'uniq_columns': 'day,video',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 30
            },
            'reach': {
                'platform': 'youtube',
                'report_name': 'reach',
                'upload_table': 'reach',
                'func_name': self.get_reach,
                'uniq_columns': 'date,video_id',
                'partitions': '',
                'merge_type': 'MergeTree',
                'refresh_type': 'delete_all',
                'history': False,
                'frequency': 'daily',
                'delay': 30
            },
            'reach_by_date': {
                'platform': 'youtube',
                'report_name': 'reach_by_date',
                'upload_table': 'reach_by_date',
                'func_name': self.get_reach_by_date,
                'uniq_columns': 'date,video_id',
                'partitions': '',
                'merge_type': 'ReplacingMergeTree(timeStamp)',
                'refresh_type': 'nothing',
                'history': True,
                'frequency': 'daily',
                'delay': 30
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

    def _get_access_token(self):
        try:
            if self.access_token and self.token_acquired_at:
                if (datetime.now() - self.token_acquired_at).total_seconds() < 3000:
                    return self.access_token
            response = httpx.post(
                'https://oauth2.googleapis.com/token',
                data={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'refresh_token': self.refresh_token,
                    'grant_type': 'refresh_token'
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get('access_token')
            self.token_acquired_at = datetime.now()
            auth_header = f'Bearer {self.access_token}'
            self.api.client.headers['Authorization'] = auth_header
            self.analytics_api.client.headers['Authorization'] = auth_header
            return self.access_token
        except Exception as e:
            self._log_err('_get_access_token', '', e)
            return None

    def _get_channel_id(self):
        if self._cached_channel_id:
            return self._cached_channel_id
        try:
            self._get_access_token()
            result = self.api._request('GET', '/youtube/v3/channels', params={'mine': 'true', 'part': 'id'})
            items = result.get('items', [])
            if not items:
                return None
            self._cached_channel_id = items[0].get('id')
            return self._cached_channel_id
        except Exception as e:
            self._log_err('_get_channel_id', '', e)
            return None

    def _get_all_video_ids(self):
        if self._cached_video_ids is not None:
            return self._cached_video_ids
        try:
            self._get_access_token()
            all_ids = []
            page_token = None
            for _ in range(500):
                params = {
                    'part': 'id,snippet',
                    'order': 'date',
                    'maxResults': '50',
                    'forMine': 'true',
                    'type': 'video'
                }
                if page_token:
                    params['pageToken'] = page_token
                result = self.api._request('GET', '/youtube/v3/search', params=params)
                items = result.get('items', []) or []
                for item in items:
                    vid_id = (item.get('id') or {}).get('videoId')
                    if vid_id:
                        all_ids.append(vid_id)
                page_token = result.get('nextPageToken')
                if not page_token:
                    break
                time.sleep(0.5)
            self._cached_video_ids = all_ids
            return all_ids
        except Exception as e:
            self._log_err('_get_all_video_ids', '', e)
            return []

    def get_videos(self, date=''):
        try:
            self._get_access_token()
            video_ids = self._get_all_video_ids()
            if not video_ids:
                return []
            all_rows = []
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i:i + 50]
                params = {
                    'part': 'id,snippet,contentDetails',
                    'maxResults': '50',
                    'id': ','.join(batch)
                }
                result = self.api._request('GET', '/youtube/v3/videos', params=params)
                items = result.get('items', []) or []
                for item in items:
                    snippet = item.get('snippet', {}) or {}
                    content = item.get('contentDetails', {}) or {}
                    tags = snippet.get('tags') or []
                    all_rows.append({
                        'id': item.get('id', ''),
                        'publishedAt': snippet.get('publishedAt', ''),
                        'title': snippet.get('title', '') or '',
                        'description': snippet.get('description', '') or '',
                        'tags': ','.join(tags) if tags else '',
                        'categoryId': snippet.get('categoryId', '') or '',
                        'defaultAudioLanguage': snippet.get('defaultAudioLanguage', '') or '',
                        'defaultLanguage': snippet.get('defaultLanguage', '') or '',
                        'duration': content.get('duration', '') or '',
                        'definition': content.get('definition', '') or '',
                        'caption': str(content.get('caption', '')).lower() == 'true',
                        'licensedContent': bool(content.get('licensedContent', False)),
                        'hasCustomThumbnail': bool(content.get('hasCustomThumbnail', False)),
                    })
                time.sleep(0.5)
            self._log_ok('get_videos', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_videos', date, e)

    def get_analytics(self, date):
        try:
            self._get_access_token()
            channel_id = self._get_channel_id()
            if not channel_id:
                return []
            video_ids = self._get_all_video_ids()
            if not video_ids:
                return []

            metrics = [
                'views', 'likes', 'dislikes', 'shares', 'comments',
                'averageViewDuration', 'estimatedMinutesWatched',
                'subscribersGained', 'subscribersLost',
                'redViews', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists'
            ]

            all_rows = []
            for i in range(0, len(video_ids), 200):
                batch = video_ids[i:i + 200]
                params = {
                    'ids': f'channel=={channel_id}',
                    'startDate': date,
                    'endDate': date,
                    'metrics': ','.join(metrics),
                    'dimensions': 'day,video',
                    'filters': 'video==' + ','.join(batch)
                }
                result = self.analytics_api._request('GET', '/v2/reports', params=params)
                column_headers = result.get('columnHeaders', []) or []
                column_names = [c.get('name', '') for c in column_headers]
                rows = result.get('rows', []) or []
                for row in rows:
                    record = dict(zip(column_names, row))
                    all_rows.append(record)
                time.sleep(0.5)

            self._log_ok('get_analytics', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_analytics', date, e)

    def get_analytics_full(self, date=''):
        try:
            self._get_access_token()
            channel_id = self._get_channel_id()
            if not channel_id:
                return []
            video_ids = self._get_all_video_ids()
            if not video_ids:
                return []

            metrics = [
                'views', 'likes', 'dislikes', 'shares', 'comments',
                'averageViewDuration', 'estimatedMinutesWatched',
                'subscribersGained', 'subscribersLost',
                'redViews', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists'
            ]

            start_date = self.start if self.start else (self.today - timedelta(days=365)).strftime('%Y-%m-%d')
            end_date = self.today.strftime('%Y-%m-%d')

            all_rows = []
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i:i + 50]
                start_index = 1
                while True:
                    params = {
                        'ids': f'channel=={channel_id}',
                        'startDate': start_date,
                        'endDate': end_date,
                        'metrics': ','.join(metrics),
                        'dimensions': 'day,video',
                        'filters': 'video==' + ','.join(batch),
                        'maxResults': '10000',
                        'startIndex': str(start_index)
                    }
                    result = self.analytics_api._request('GET', '/v2/reports', params=params)
                    column_headers = result.get('columnHeaders', []) or []
                    column_names = [c.get('name', '') for c in column_headers]
                    rows = result.get('rows', []) or []
                    if not rows:
                        break
                    for row in rows:
                        all_rows.append(dict(zip(column_names, row)))
                    if len(rows) < 10000:
                        break
                    start_index += 10000
                    time.sleep(0.5)
                time.sleep(0.5)

            self._log_ok('get_analytics_full', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_analytics_full', date, e)

    def _list_reporting_jobs(self):
        try:
            self._get_access_token()
            result = self.reporting_api._request('GET', '/v1/jobs')
            return result.get('jobs', []) or []
        except Exception as e:
            self._log_err('_list_reporting_jobs', '', e)
            return []

    def _ensure_reach_job(self):
        try:
            self._get_access_token()
            jobs = self._list_reporting_jobs()
            for job in jobs:
                if job.get('reportTypeId') == 'channel_reach_basic_a1':
                    return job.get('id')
            result = self.reporting_api._request('POST', '/v1/jobs', json={
                'reportTypeId': 'channel_reach_basic_a1',
                'name': f'morin_reach_{self.add_name}'
            })
            return result.get('id')
        except Exception as e:
            self._log_err('_ensure_reach_job', '', e)
            return None

    def _list_reach_reports(self, job_id):
        try:
            self._get_access_token()
            all_reports = []
            page_token = None
            for _ in range(200):
                params = {}
                if page_token:
                    params['pageToken'] = page_token
                result = self.reporting_api._request('GET', f'/v1/jobs/{job_id}/reports', params=params)
                all_reports.extend(result.get('reports', []) or [])
                page_token = result.get('nextPageToken')
                if not page_token:
                    break
                time.sleep(0.3)
            return all_reports
        except Exception as e:
            self._log_err('_list_reach_reports', '', e)
            return []

    def _download_reach_csv(self, download_url):
        try:
            self._get_access_token()
            response = httpx.get(
                download_url,
                headers={'Authorization': f'Bearer {self.access_token}'},
                timeout=120,
                follow_redirects=True
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            self._log_err('_download_reach_csv', '', e)
            return ''

    def _csv_to_rows(self, csv_text):
        if not csv_text or not csv_text.strip():
            return []
        rows = []
        reader = csv_module.DictReader(StringIO(csv_text))
        for row in reader:
            rows.append(row)
        return rows

    def get_reach(self, date=''):
        try:
            job_id = self._ensure_reach_job()
            if not job_id:
                return []
            reports = self._list_reach_reports(job_id)
            if not reports:
                self._log_ok('get_reach (no reports yet)', date)
                return []
            all_rows = []
            for r in reports:
                download_url = r.get('downloadUrl')
                if not download_url:
                    continue
                csv_text = self._download_reach_csv(download_url)
                rows = self._csv_to_rows(csv_text)
                all_rows.extend(rows)
                time.sleep(0.5)
            self._log_ok('get_reach', date)
            return all_rows
        except Exception as e:
            return self._log_err('get_reach', date, e)

    def get_reach_by_date(self, date):
        try:
            job_id = self._ensure_reach_job()
            if not job_id:
                return []
            reports = self._list_reach_reports(job_id)
            if not reports:
                return []
            target = None
            for r in reports:
                start = r.get('startTime', '') or ''
                if start.startswith(date):
                    target = r
                    break
            if not target:
                return []
            csv_text = self._download_reach_csv(target.get('downloadUrl', ''))
            rows = self._csv_to_rows(csv_text)
            self._log_ok('get_reach_by_date', date)
            return rows
        except Exception as e:
            return self._log_err('get_reach_by_date', date, e)

    def collecting_manager(self):
        report_list = self.reports.replace(' ', '').lower().split(',')
        for report in report_list:
            if report not in self.source_dict:
                continue
            self.clickhouse = make_db(
                self.subd, self.bot_token, self.chat_list, self.message_type,
                self.host, self.port, self.username, self.password, self.database,
                self.start, self.add_name, self.err429, self.backfill_days, self.platform
            )
            cfg = self.source_dict[report]
            self.clickhouse.collecting_report(
                cfg['platform'], cfg['report_name'], cfg['upload_table'],
                cfg['func_name'], cfg['uniq_columns'], cfg['partitions'],
                cfg['merge_type'], cfg['refresh_type'], cfg['history'],
                cfg['frequency'], cfg['delay']
            )
        self.common.send_logs_clear_anyway(self.bot_token, self.chat_list)
