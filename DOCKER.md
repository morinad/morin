# Запуск базы данных и загрузки данных в Docker
## Установка Docker на компьютер:
1) Установите Docker Desktop для вашей операционной системы
2) Указывайте BASE_DIR (инструкции ниже) для указания папки с настройками

## Установка Docker на сервер:
sudo apt update

sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update

sudo apt install -y docker-ce

sudo systemctl status docker (это проверка статуса, после проверки нажмите Ctrl+C чтобы вернуться в командную строку)


## Подготовка volume и сети:
docker volume create clickhouse_volume

docker network create --driver bridge chnet


## Запуск контейнера Clickhouse:
Если работаете на Windows, укажите папку с файлами настроек:

set BASE_DIR=C:/Github/exp_scripts/Применение Docker


**Для Windows (папка %BASE_DIR%):**

docker run -d --name my_clickhouse --network chnet -p 8123:8123 -p 9000:9000 -v clickhouse_volume:/var/lib/clickhouse 
-v "%BASE_DIR%/config.xml:/etc/clickhouse-server/config.xml" -v "%BASE_DIR%/users.xml:/etc/clickhouse-server/users.xml" --user clickhouse morinad/my_clickhouse clickhouse-server --config-file=/etc/clickhouse-server/config.xml


**Для Linux (используем папку /home):**

docker run -d --name my_clickhouse --network chnet -p 8123:8123 -p 9000:9000 -v clickhouse_volume:/var/lib/clickhouse 
-v "/home/config.xml:/etc/clickhouse-server/config.xml" -v "/home/users.xml:/etc/clickhouse-server/users.xml" --user clickhouse morinad/my_clickhouse clickhouse-server --config-file=/etc/clickhouse-server/config.xml


## Запуск загрузки данных из API:
**Для Windows (папка %BASE_DIR%):**

docker run -d --name upload_data --network chnet -v "%BASE_DIR%/settings.xlsx:/app/settings.xlsx" morinad/upload_data


**Для Linux (используем папку /home):**

docker run -d --name upload_data --network chnet -v "/home/settings.xlsx:/app/settings.xlsx" morinad/upload_data
