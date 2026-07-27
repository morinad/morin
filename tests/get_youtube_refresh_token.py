import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from morin import YOUTUBEbyDate

code = ''  # ← вставь сюда code из адресной строки после авторизации

result = YOUTUBEbyDate.get_refresh_token(code)
print(result)
