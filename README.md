# Коннекторы для получения данных из API + загркзка в Clickhouse

Коннекторы дают возможность загружать данные из разных API. Проект работает на базе Docker и собирает данные в Clickhouse, но может также использоваться как обычная библиотека для получения данных без привязки к Docker или Clickhouse. 

### Ключевые преимущества:
- автоматическое определение состава и типов столбцов, не нужно менять код при добавлении или удалении столбцов на стороне API
- быстрое добавление стандартных методов
- инкрементное обновление (получение N последних дней без перезагрузки всех данных)
- централизованные правки при изменениях API (пользователю надо просто перезапустить контейнер)
- может работать как для Clickhouse, так и для обычного получения данных (пример - получение данных в Google Sheets, видео ниже)
- уведомления об ошибках и ходе загрузки в Telegram

### Список источников: 
WB, OZON, Yandex Market, GetCourse, Yandex Direct, VK Ads, Yandex Metrika, Bitrix24.

### Полезные ссылки:
- Подробная статья: http://directprobi.ru/blogs/dannye-api-v-clickhouse-na-python-docker-connectors-wb-ozon-yandex/
- Основное видео с демонстрацией: https://youtu.be/OHwtIrQyF68
- Ответы на вопросы: https://youtu.be/sK4mHmAbu4A
- Все полезные файлы на Boosty: https://boosty.to/morinad/posts/ae00106e-4e29-4010-9aff-433d28b620f8?share=post_link
- Применение коннекторов для Google Sheets без Clickhouse и Docker: https://youtu.be/KvuaLI8mGDU

### Новые видео, статьи и полезности в Telegram-канале: https://t.me/+2kqVrjV5aXs0NTRi

### Наши курсы по Power Query, Power BI и автоматизации:
1) Основы Power BI (бесплатный курс): https://directprorf.ru/basics?utm_source=github
2) Power BI для рекламных отчётов: https://directprorf.ru/powerbi?utm_source=github
3) Из API в Excel и Power BI + коннекторы: https://directprorf.ru/excel?utm_source=github
4) Коннекторы для Маркетплейсов: https://directprorf.ru/marketplaces?utm_source=github
5) Продвинутый Power Query: https://directprorf.ru/pro?utm_source=github
6) Создание коннекторов в Power Query: https://directprorf.ru/connectors?utm_source=github

