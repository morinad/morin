import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def _is_retryable(e):
    if isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code == 429 or e.response.status_code >= 500
    return False


class BaseMarketplaceClient:
    def __init__(self, base_url='', headers=None, bot_token='', chat_list=None, common=None, name=''):
        self.client = httpx.Client(base_url=base_url, headers=headers or {}, timeout=30.0)
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
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                self.err429 = True
            raise e
        except Exception as e:
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
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                self.err429 = True
            raise e
        except Exception as e:
            self._log_error(endpoint, e)
            raise

    def _log_error(self, func_name, error):
        message = f'Платформа: API. Имя: {self.name}. Функция: {func_name}. Ошибка: {error}.'
        self.common.log_func(self.bot_token, self.chat_list, message, 3)

    def close(self):
        self.client.close()
