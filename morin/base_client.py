from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False
    import requests


def _is_retryable(e):
    if HAS_HTTPX and isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code == 429 or e.response.status_code >= 500
    if not HAS_HTTPX and isinstance(e, requests.HTTPError):
        if e.response is not None:
            return e.response.status_code == 429 or e.response.status_code >= 500
    return False


class _RequestsClientWrapper:
    def __init__(self, base_url='', headers=None, timeout=30.0):
        self._base_url = base_url.rstrip('/')
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(headers or {})
        self.headers = self._session.headers

    @property
    def base_url(self):
        return self._base_url

    def request(self, method, url, **kwargs):
        full_url = f'{self._base_url}{url}' if self._base_url else url
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self._timeout
        extra_headers = kwargs.pop('headers', None)
        if extra_headers:
            merged = dict(self._session.headers)
            merged.update(extra_headers)
            kwargs['headers'] = merged
        response = self._session.request(method, full_url, **kwargs)
        return response

    def close(self):
        self._session.close()


class BaseMarketplaceClient:
    def __init__(self, base_url='', headers=None, bot_token='', chat_list=None, common=None, name=''):
        if HAS_HTTPX:
            self.client = httpx.Client(base_url=base_url, headers=headers or {}, timeout=30.0)
        else:
            self.client = _RequestsClientWrapper(base_url=base_url, headers=headers or {}, timeout=30.0)
        self.bot_token = bot_token
        self.chat_list = chat_list
        self.common = common
        self.name = name
        self.err429 = False

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception(_is_retryable),
        reraise=True
    )
    def _request(self, method, endpoint, **kwargs):
        try:
            response = self.client.request(method, endpoint, **kwargs)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            status = None
            if HAS_HTTPX and isinstance(e, httpx.HTTPStatusError):
                status = e.response.status_code
            elif not HAS_HTTPX and isinstance(e, requests.HTTPError):
                if e.response is not None:
                    status = e.response.status_code
            if status == 429:
                self.err429 = True
            if status is not None:
                raise
            self._log_error(endpoint, e)
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception(_is_retryable),
        reraise=True
    )
    def _request_raw(self, method, endpoint, **kwargs):
        try:
            response = self.client.request(method, endpoint, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            status = None
            if HAS_HTTPX and isinstance(e, httpx.HTTPStatusError):
                status = e.response.status_code
            elif not HAS_HTTPX and isinstance(e, requests.HTTPError):
                if e.response is not None:
                    status = e.response.status_code
            if status == 429:
                self.err429 = True
            if status is not None:
                raise
            self._log_error(endpoint, e)
            raise

    def _log_error(self, func_name, error):
        if self.common:
            message = f'Платформа: API. Имя: {self.name}. Функция: {func_name}. Ошибка: {error}.'
            self.common.log_func(self.bot_token, self.chat_list, message, 3)

    def close(self):
        self.client.close()
