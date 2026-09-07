
<div align="center">

# ⚡ Weather ETL Pipeline with Apache Airflow

<br>

<a href="README.md">
  <img src="https://img.shields.io/badge/🇬🇧_English-README-blue?style=for-the-badge&logo=markdown&logoColor=white" alt="English">
</a>
<a href="README.ru.md">
  <img src="https://img.shields.io/badge/🇷🇺_Русский-README-red?style=for-the-badge&logo=markdown&logoColor=white" alt="Русский">
</a>

<br><br>

![версия](https://img.shields.io/badge/version-1.0.0-blue)
![лицензия](https://img.shields.io/badge/license-MIT-green)
![статус](https://img.shields.io/badge/status-active-brightgreen)

</div>

# 📊 Полный гайд по развертыванию и использованию

## 📋 Оглавление

   * [Описание проекта](#-Описание-проекта)

   * [Архитектура](#-Архитектура)

   * Предварительные требования

   * Установка и настройка

   * Структура проекта

   * Компоненты системы

   * Работа с DAG

   * Тестирование

   * Мониторинг и отладка

   * Производственный деплой

   * Устранение неполадок

   * Команды для разработки

   * Лицензия

 ###  📖 
 ### Описание проекта

Weather ETL Pipeline — это полностью автоматизированная система для сбора, обработки и хранения погодных данных с использованием Open-Meteo API. 
Система построена на основе Apache Airflow и запускается в Docker-контейнерах, обеспечивая изоляцию, масштабируемость и надежность.
### 🎯 Основные возможности

    ✅ Автоматический сбор данных — загрузка исторических и прогнозных данных каждые 30 минут

    ✅ Два режима загрузки — исторические данные (при первом запуске) и прогнозные данные (регулярно)

    ✅ Хранение в PostgreSQL — все данные сохраняются с временными метками в UTC

    ✅ Веб-интерфейс Airflow — удобный мониторинг и управление DAG

    ✅ Docker-изоляция — все сервисы запускаются в контейнерах

    ✅ Обработка ошибок — автоматические повторные попытки при сбоях

    ✅ Логирование — подробные логи всех операций

### 📊 
### Источники данных

   * Open-Meteo Historical API — архивные данные с 2020 года

   * Open-Meteo Forecast API — прогнозные данные на 7 дней
### 🗄️ 
### Хранимые параметры

* 🌡️ Температура на высоте 2м

* 💧 Относительная влажность

* 🌡️ Точка росы

* 🌡️ Кажущаяся температура

* 🌧️ Осадки, дождь, снег

* ❄️ Глубина снега

* 🏷️ Код погоды

* 📊 Давление (MSL и поверхностное)

* ☁️ Облачность (общая, низкая, средняя, высокая)

* 💨 Ветер (скорость на 10м и 100м, направление, порывы)

* 🌱 Почвенные параметры (температура и влажность на разных глубинах)

## 🏗️ 
### Архитектура
### Диаграмма системы  
<img width="1729" height="706" alt="image" src="https://github.com/user-attachments/assets/dc0725b2-ee6b-4475-a2d7-df37dbae038b" />
     
## Поток данных

<img width="1206" height="899" alt="image" src="https://github.com/user-attachments/assets/03e5bf87-e06c-493e-b9e4-05b5c25b6e52" />

## 📋 
### Предварительные требования
### Системные требования
|Компонент         |	Минимальная версия	|Рекомендуемая версия
|------------------|-----------------------|--------------------|
Docker	           |20.10.x	              |24.0.x+
Docker Compose	   |2.0.x               	|2.20.x+
Python	           |3.8	                  |3.10+
RAM	               |4 GB                  |8 GB+
Disk Space	       |10 GB	                |20 GB+

### Поддерживаемые ОС

  * ✅ Windows 10/11 (с WSL2)

  * ✅ macOS 10.15+

  * ✅ Linux (Ubuntu 20.04+, Debian 11+, CentOS 8+)

### Установка Docker
* Windows (с WSL2)
 powershell

 1. Установите WSL2
```bach 
wsl --install


 2. Перезагрузите компьютер

 3. Скачайте и установите Docker Desktop

 https://www.docker.com/products/docker-desktop/

 4. Включите WSL2 в настройках Docker Desktop

 Settings → General → Use WSL 2 based engine ✅
```
### macOS
```bash

 1. Скачайте и установите Docker Desktop
 https://www.docker.com/products/docker-desktop/

 2. Или через Homebrew
brew install --cask docker
```
### Linux (Ubuntu/Debian)
```bach 

1. Обновите систему
sudo apt update && sudo apt upgrade -y

 2. Установите Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

 3. Добавьте пользователя в группу docker
sudo usermod -aG docker $USER

 4. Перезагрузитесь или выполните
newgrp docker

 5. Установите Docker Compose Plugin
sudo apt install docker-compose-plugin -y

### Проверка установки
```bach 

 Проверка Docker
docker --version
 Вывод: Docker version 24.0.7, build afdd53b

 Проверка Docker Compose
docker-compose --version
 Вывод: Docker Compose version v2.23.0

 Проверка, что Docker работает
docker run hello-world
```
## 📂 
## Структура проекта
```bach
Mteo_Weather_AirFlow/
│
├── dags/                                    # DAG файлы Airflow
│   ├── __init__.py
│   ├── weather_etl_complete.py             # Основной DAG (159 строк)
│   └── test_dag.py                         # Тестовый DAG
│
├── scripts/                                 # Python скрипты ETL
│   ├── __init__.py
│   ├── historical_weather_loader.py       # Загрузка истории (~450 строк)
│   └── forecast_weather_loader.py         # Загрузка прогноза (~450 строк)
│
├── plugins/                                 # Плагины Airflow
│   ├── __init__.py
│   └── weather_utils.py                   # Утилиты для работы с БД
│
├── docker/                                  # Docker конфигурация
│   └── requirements.txt                    # Python зависимости
│
├── init-scripts/                            # Скрипты инициализации БД
│   └── 01_create_weather_table.sql        # Создание таблицы
│
├── logs/                                    # Логи Airflow
├── data/                                    # Данные (кеш API)
├── .env                                     # Переменные окружения
├── .gitignore                               # Git игнорирование
├── docker-compose.yml                       # Docker Compose конфигурация
├── Dockerfile                               # Docker образ Airflow
├── Makefile                                 # Автоматизация команд
└── README.md                                # Документация
```
###🔧 
### Установка и настройка

#### 1. Клонирование репозитория
```bash

SSH
git clone git@github.com:yourusername/Mteo_Weather_AirFlow.git

HTTPS
git clone https://github.com/yourusername/Mteo_Weather_AirFlow.git

 Переход в проект
cd Mteo_Weather_AirFlow
```
#### 2. Создание файлов конфигурации
#### .env файл
```bash

Создайте .env файл
cat > .env << EOF
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
WEBSERVER_SECRET_KEY=your_super_secret_key_change_me
AIRFLOW_UID=50000
AIRFLOW_GID=50000
EOF
```

#### docker/requirements.txt
```bash

mkdir -p docker
cat > docker/requirements.txt << EOF
apache-airflow==2.7.0
apache-airflow-providers-postgres==5.5.0
openmeteo-requests==1.0.0
pandas==2.0.3
numpy==1.24.3
requests-cache==1.1.0
retry-requests==1.0.0
sqlalchemy==1.4.47
psycopg2-binary==2.9.7
pytz==2023.3
python-dotenv==1.0.0
EOF
```
#### 3. Создание SQL таблицы
```bash
init-scripts/01_create_weather_table.sql
sql

-- Создание схемы
-- Table: weather_data.historical_weather

-- DROP TABLE IF EXISTS weather_data.historical_weather;

CREATE TABLE IF NOT EXISTS weather_data.historical_weather
(
    id bigint NOT NULL DEFAULT nextval('weather_data.historical_weather_id_seq'::regclass),
    timestamp_utc timestamp with time zone NOT NULL,
                                timezone_offset character varying(10) COLLATE pg_catalog."default" DEFAULT '+3'::character varying,
    temperature_2m double precision,
    relative_humidity_2m double precision,
    dew_point_2m double precision,
    apparent_temperature double precision,
    precipitation double precision,
    rain double precision,
    snowfall double precision,
    snow_depth double precision,
    weather_code integer,
    pressure_msl double precision,
    surface_pressure double precision,
    cloud_cover double precision,
    cloud_cover_low double precision,
    cloud_cover_mid double precision,
    cloud_cover_high double precision,
    et0_fao_evapotranspiration double precision,
    vapour_pressure_deficit double precision,
    wind_speed_10m double precision,
    wind_speed_100m double precision,
    wind_direction_10m double precision,
    wind_direction_100m double precision,
    wind_gusts_10m double precision,
    soil_temperature_0_to_7cm double precision,
    soil_temperature_7_to_28cm double precision,
    soil_temperature_28_to_100cm double precision,
    soil_temperature_100_to_255cm double precision,
    soil_moisture_0_to_7cm double precision,
    soil_moisture_7_to_28cm double precision,
    soil_moisture_28_to_100cm double precision,
    soil_moisture_100_to_255cm double precision,
    latitude double precision,
    longitude double precision,
    location_name character varying(100) COLLATE pg_catalog."default",
    data_source character varying(100) COLLATE pg_catalog."default",
    load_start_date date,
    load_end_date date,
    hour_utc integer,
    day_of_week_utc integer,
    month_utc integer,
    year_utc integer,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
                                CONSTRAINT historical_weather_pkey PRIMARY KEY (id)
    )

    TABLESPACE pg_default;

ALTER TABLE IF EXISTS weather_data.historical_weather
    OWNER to postgres;

COMMENT ON TABLE weather_data.historical_weather
    IS 'Таблица с историческими и прогнозными погодными данными';

COMMENT ON COLUMN weather_data.historical_weather.timestamp_utc
    IS 'Время в UTC (без смещения)';

COMMENT ON COLUMN weather_data.historical_weather.temperature_2m
    IS 'Температура на высоте 2м в °C';

COMMENT ON COLUMN weather_data.historical_weather.relative_humidity_2m
    IS 'Относительная влажность на высоте 2м в %';
-- Index: idx_weather_location

-- DROP INDEX IF EXISTS weather_data.idx_weather_location;

CREATE INDEX IF NOT EXISTS idx_weather_location
    ON weather_data.historical_weather USING btree
    (latitude ASC NULLS LAST, longitude ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_weather_temp

-- DROP INDEX IF EXISTS weather_data.idx_weather_temp;

CREATE INDEX IF NOT EXISTS idx_weather_temp
    ON weather_data.historical_weather USING btree
    (temperature_2m ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_weather_timestamp_utc

-- DROP INDEX IF EXISTS weather_data.idx_weather_timestamp_utc;

CREATE INDEX IF NOT EXISTS idx_weather_timestamp_utc
    ON weather_data.historical_weather USING btree
    (timestamp_utc ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_weather_unique

-- DROP INDEX IF EXISTS weather_data.idx_weather_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique
    ON weather_data.historical_weather USING btree
    (timestamp_utc ASC NULLS LAST, latitude ASC NULLS LAST, longitude ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
```
#### 4. Dockerfile
```bash
dockerfile

FROM apache/airflow:2.7.0-python3.10

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir -p /opt/airflow/dags /opt/airflow/scripts /opt/airflow/plugins /opt/airflow/logs

COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY plugins/ /opt/airflow/plugins/

USER root
RUN chown -R airflow:airflow /opt/airflow/dags /opt/airflow/scripts /opt/airflow/plugins /opt/airflow/logs
USER airflow

ENV PYTHONPATH=/opt/airflow
```
#### 5. docker-compose.yml
```bash
yaml
services:
  weather_postgres:
    image: postgres:15-alpine
    container_name: weather_postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: 12345
      POSTGRES_DB: weather_db
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      - "5434:5432"  # Используем свободный порт 5434
    volumes:
      - weather_postgres_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d weather_db"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - airflow_network
    restart: unless-stopped

  airflow_postgres:
    image: postgres:15-alpine
    container_name: airflow_postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5435:5432"  # Используем свободный порт 5435
    volumes:
      - airflow_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - airflow_network
    restart: unless-stopped

  redis:
    image: redis:7.2-alpine
    container_name: redis
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - airflow_network
    restart: unless-stopped

  airflow:
    image: apache/airflow:2.7.0
    container_name: airflow
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow_postgres:5432/airflow
      - AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY:-46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=}
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__RBAC=True
      - AIRFLOW__WEBSERVER__SECRET_KEY=${WEBSERVER_SECRET_KEY:-your_super_secret_key}
      - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
      - AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Europe/Moscow
      - AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=60
      - AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=60
      - AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
      - AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth
      - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
      - PYTHONPATH=/opt/airflow
      - TZ=Europe/Moscow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
      - ./docker/requirements.txt:/requirements.txt
    ports:
      - "8080:8080"
    command: >
      bash -c "
        pip install --no-cache-dir -r /requirements.txt &&
        airflow db init &&
        airflow users create \
          --username admin \
          --password admin123 \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com || echo 'User already exists' &&
        airflow webserver & airflow scheduler
      "
    depends_on:
      airflow_postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - airflow_network
    restart: unless-stopped

```
### 🚀 
### Запуск проекта
#### Быстрый старт
```bash
 1. Сборка образов
docker-compose build

 2. Запуск всех сервисов
docker-compose up -d

 3. Проверка статуса
docker-compose ps

 4. Просмотр логов
docker-compose logs -f
```
####  Пошаговый запуск
```bash
 1. Проверка Docker
docker --version
docker-compose --version

 2. Создание необходимых папок
mkdir -p dags scripts plugins logs init-scripts docker

 3. Копирование файлов в контейнер (если нужно)
docker cp dags/weather_etl_complete.py airflow:/opt/airflow/dags/
docker cp scripts/historical_weather_loader.py airflow:/opt/airflow/scripts/
docker cp scripts/forecast_weather_loader.py airflow:/opt/airflow/scripts/
docker cp plugins/weather_utils.py airflow:/opt/airflow/plugins/

 4. Перезапуск Airflow
docker restart airflow_webserver
docker restart airflow_scheduler

 5. Проверка работы DAG
docker exec airflow_webserver airflow dags list | grep weather
```
#### Проверка работы
```bash
 1. Проверка PostgreSQL
docker exec -it weather_postgres psql -U postgres -d weather_db -c "\dt weather_data.*"

 2. Проверка Redis
docker exec -it airflow_redis redis-cli ping

 3. Проверка Airflow
curl http://localhost:8080/health

 4. Открытие Airflow UI
 Откройте браузер: http://localhost:8080
 Логин: admin, Пароль: admin123
```

## 📊 
## Компоненты системы
### 1. DAG (weather_etl_complete.py)

#### Местоположение: dags/weather_etl_complete.py (159 строк)

#### Описание: 
Основной оркестратор, управляющий процессом ETL.

#### Структура:
```bash
 1. Импорты (10 строк)
 2. Настройки DAG (20 строк)  
 3. Функции-обертки (40 строк)
 4. Определение задач (30 строк)
 5. Зависимости (10 строк)
```
#### Логика работы:
```bash
def check_and_choose_branch():
    """Проверяет таблицу и выбирает ветку"""
    if таблица пуста:
        return 'historical_load'    # Загрузка истории
    else:
        return 'forecast_load'      # Загрузка прогноза

```
#### Граф зависимостей:
```bash
branch_check_table
        ↓
   ┌────┴────┐
   ↓         ↓
historical_load  forecast_load
   ↓         ↓
   └────┬────┘
        ↓
  log_complete
        ↓
   end_pipeline
```
### 2. Historical Loader (historical_weather_loader.py)

#### Местоположение: 
scripts/historical_weather_loader.py (~450 строк)

#### Что делает:

  *  Загружает архивные данные за последние 30 дней

  *  Проверяет, какие дни уже загружены

  *  Сохраняет данные по дням в PostgreSQL

#### Основные функции:
```bash
def get_existing_dates():
    """Получение уже загруженных дат"""
    # Возвращает список дат, уже присутствующих в БД

def get_historical_data_for_date(date_str):
    """Загрузка данных за конкретный день"""
    # Запрос к Open-Meteo Archive API

def save_to_postgres(df):
    """Сохранение данных в PostgreSQL"""
    # Вставка данных с проверкой типов
```
### 3. Forecast Loader (forecast_weather_loader.py)

#### Местоположение: 
scripts/forecast_weather_loader.py (~450 строк)

#### Что делает:

   *  Загружает прогнозные данные на сегодня

   *  Добавляет только новые записи (без дубликатов)

   *  Сохраняет в PostgreSQL

#### Особенности:

   *  Использует requests вместо openmeteo_requests для стабильности

   *  Проверяет дубликаты перед вставкой

   *  Обрабатывает ошибки API

### 4. Weather Utils (weather_utils.py)

#### Местоположение: 
plugins/weather_utils.py (~80 строк)

#### Описание: 
Утилиты для работы с базой данных.

#### Функции:
```bash
def get_db_engine():
    """Создание подключения к БД"""
    # Возвращает SQLAlchemy engine

def check_table_empty():
    """Проверка, пустая ли таблица"""
    # Возвращает True/False

def get_last_timestamp():
    """Получение последней временной метки"""
    # Возвращает datetime

def get_table_stats():
    """Получение статистики по таблице"""
    # Возвращает словарь со статистикой
```
### 🔄 
### Работа с DAG
#### Запуск DAG

#### Через Web UI:

  *  Откройте http://localhost:8080

  *  Логин: admin / admin123

  *  Найдите DAG weather_etl_complete

  *  Включите переключатель (ON)

  *  Нажмите ▶️ Trigger DAG

#### Через CLI:
```bash
# Запуск DAG
docker exec airflow_webserver airflow dags trigger weather_etl_complete

# Просмотр статуса
docker exec airflow_webserver airflow dags state weather_etl_complete

# Просмотр списка DAG
docker exec airflow_webserver airflow dags list
```
### Мониторинг выполнения

#### Web UI:

  *  Grid View — таблица всех запусков

  *  Graph View — визуализация зависимостей

  *  Tree View — иерархический просмотр

  *  Log — логи каждой задачи

#### CLI:
```bash
# Просмотр логов конкретной задачи
docker exec airflow_webserver airflow tasks logs \
    weather_etl_complete forecast_load \
    2024-01-01T00:00:00+00:00

# Проверка статуса задач
docker exec airflow_webserver airflow tasks states-for-dag-run \
    weather_etl_complete manual__2024-01-01T00:00:00+00:00

```
#### Настройка расписания
```bash
# Каждые 30 минут
schedule_interval='*/30 * * * *'

# Каждый час
schedule_interval='@hourly'

# Каждый день в полночь
schedule_interval='@daily'

# Каждое воскресенье в 00:00
schedule_interval='0 0 * * 0'

# По расписанию cron
schedule_interval='0 0,12 * * *'  # Дважды в день
```
### 🧪 
### Тестирование
```bash
# test_api_connection.py
import requests

def test_openmeteo_api():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': 55.5764,
        'longitude': 37.4365,
        'current': 'temperature_2m'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"✅ API работает! Температура: {data['current']['temperature_2m']}°C")
        return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

if __name__ == '__main__':
    test_openmeteo_api()
```
### Тест скриптов
```bash
# Тест исторической загрузки
docker exec airflow_webserver python /opt/airflow/scripts/historical_weather_loader.py

# Тест прогнозной загрузки
docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py

# Тест с полным логированием
docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py 2>&1 | tee /tmp/test.log
```
### Тест базы данных
```bash
-- Проверка структуры таблицы
\d weather_data.historical_weather

-- Проверка данных
SELECT 
    COUNT(*) as total,
    data_source,
    MIN(timestamp_utc) as first_date,
    MAX(timestamp_utc) as last_date
FROM weather_data.historical_weather
GROUP BY data_source;

-- Проверка последних записей
SELECT 
    timestamp_utc,
    temperature_2m,
    relative_humidity_2m,
    data_source
FROM weather_data.historical_weather 
ORDER BY timestamp_utc DESC 
LIMIT 10;

-- Проверка дубликатов
SELECT 
    timestamp_utc, 
    COUNT(*) 
FROM weather_data.historical_weather
GROUP BY timestamp_utc
HAVING COUNT(*) > 1;
```

### Интеграционные тесты
```bash
# 1. Запуск DAG в тестовом режиме
docker exec airflow_webserver airflow dags test weather_etl_complete 2024-01-01

# 2. Тест конкретной задачи
docker exec airflow_webserver airflow tasks test \
    weather_etl_complete forecast_load 2024-01-01

# 3. Просмотр логов теста
docker exec airflow_webserver airflow tasks logs \
    weather_etl_complete forecast_load 2024-01-01
```
## 📊 
## Мониторинг и отладка
### Логи
```bash
# Логи всех контейнеров
docker-compose logs -f

# Логи конкретного контейнера
docker logs -f airflow_webserver
docker logs -f airflow_scheduler
docker logs -f weather_postgres

# Логи с фильтром по ошибкам
docker logs airflow_webserver 2>&1 | grep -i error
docker logs airflow_webserver 2>&1 | grep -i traceback

# Последние N строк
docker logs airflow_webserver --tail 100
```
### Метрики и мониторинг
```bash
-- Количество записей по дням
SELECT 
    DATE(timestamp_utc) as date,
    COUNT(*) as records,
    ROUND(AVG(temperature_2m)::numeric, 1) as avg_temp
FROM weather_data.historical_weather
GROUP BY DATE(timestamp_utc)
ORDER BY date DESC;

-- Статистика по источникам данных
SELECT 
    data_source,
    COUNT(*) as total,
    MIN(timestamp_utc) as first_date,
    MAX(timestamp_utc) as last_date
FROM weather_data.historical_weather
GROUP BY data_source;

-- Проверка пропусков данных
SELECT 
    DATE(timestamp_utc) as date,
    COUNT(*) as hours,
    CASE 
        WHEN COUNT(*) < 24 THEN '⚠️ Неполные данные'
        ELSE '✅ Полные данные'
    END as status
FROM weather_data.historical_weather
GROUP BY DATE(timestamp_utc)
ORDER BY date DESC
LIMIT 7;
```
### Отладка DAG
```bash
# Добавление отладочных сообщений
def load_forecast():
    logger.info("🚀 Начало загрузки прогноза")
    try:
        from scripts.forecast_weather_loader import main as forecast_main
        logger.info("📦 Модуль импортирован")
        result = forecast_main()
        logger.info(f"📊 Результат: {result}")
        return result
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        raise
```

### 🏭 
### Производственный диплой
#### Настройки для продакшена
```bash
# docker-compose.prod.yml
services:
  weather_postgres:
    environment:
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: ${DB_NAME}
    volumes:
      - /data/postgres:/var/lib/postgresql/data

  airflow_webserver:
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__WEBSERVER__SECRET_KEY=${WEBSERVER_SECRET_KEY}
      - AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING
    restart: always

```

### Безопасность
```bash
# Генерация secure ключей
openssl rand -base64 32  # FERNET_KEY
openssl rand -hex 32     # SECRET_KEY
--------------
# Настройка .env
cat > .env << EOF
FERNET_KEY=ваш_ключ
WEBSERVER_SECRET_KEY=ваш_секретный_ключ
DB_USER=ваш_пользователь
DB_PASSWORD=ваш_пароль
DB_NAME=weather_db
EOF
--------------
# Ограничение прав доступа
chmod 600 .env
chmod 644 docker-compose.yml
```
### Масштабирование
```bash
# docker-compose.scale.yml
services:
  airflow-worker:
    image: apache/airflow:2.7.0
    deploy:
      replicas: 3
      resources:
        limits:
          memory: 2GB
    environment:
      - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
```
### 🐛 
### Устранение неполадок
#### Распространенные проблемы и решения
#### 1. Контейнеры не запускаются
```bash
# Проверка логов
docker-compose logs

# Перезапуск с очисткой
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d

# Проверка портов
netstat -ano | findstr :5434  # Windows
sudo lsof -i :5434             # Mac/Linux
```
#### 2. Ошибка подключения к БД
```bash
# Проверка статуса PostgreSQL
docker ps | grep postgres

# Проверка подключения
docker exec -it weather_postgres psql -U postgres -d weather_db -c "SELECT 1;"

# Перезапуск PostgreSQL
docker-compose restart weather_postgres
```
#### 3. Ошибка ModuleNotFoundError
```bash
# Проверка установленных пакетов
docker exec airflow_webserver pip list

# Установка пакетов
docker exec airflow_webserver pip install pandas numpy sqlalchemy

# Проверка импортов
docker exec airflow_webserver python -c "import pandas; print('OK')"
```
#### 4. API таймаут
```bash
# Проверка доступа к API
docker exec airflow_webserver curl -I https://api.open-meteo.com/v1/forecast

# Проверка DNS
docker exec airflow_webserver nslookup api.open-meteo.com

# Увеличение таймаута в скрипте
params = {'timeout': 60}  # вместо 30
```
#### 5. DAG не появляется в UI
```bash
# Проверка синтаксиса
docker exec airflow_webserver python -m py_compile /opt/airflow/dags/weather_etl_complete.py

# Проверка логирования
docker logs airflow_webserver | grep -i "weather_etl_complete"

# Принудительная перезагрузка DAG
docker exec airflow_webserver airflow dags list | grep weather
```
#### Восстановление данных
```bash
# Бэкап базы данных
docker exec -t weather_postgres pg_dump -U postgres weather_db > backup.sql

# Восстановление
cat backup.sql | docker exec -i weather_postgres psql -U postgres weather_db

# Очистка таблицы
docker exec -it weather_postgres psql -U postgres -d weather_db -c "
TRUNCATE TABLE weather_data.historical_weather CASCADE;
"
```
### 📝 
### Команды для разработки
#### Работа с Docker
```bash

# Управление контейнерами
docker-compose up -d          # Запуск
docker-compose down           # Остановка
docker-compose restart        # Перезапуск
docker-compose ps             # Статус

# Работа с образами
docker-compose build          # Сборка
docker-compose build --no-cache  # Сборка без кеша
docker rmi $(docker images -q)  # Удаление всех образов

# Работа с томами
docker volume ls
docker volume prune -f
docker-compose down -v        # Удаление томов

# Логи
docker-compose logs -f        # Все логи
docker-compose logs airflow_webserver -f  # Логи конкретного сервиса

```
#### Работа со скриптами
```bash
# Копирование файлов
docker cp dags/weather_etl_complete.py airflow_webserver:/opt/airflow/dags/
docker cp scripts/forecast_weather_loader.py airflow_webserver:/opt/airflow/scripts/

# Запуск скриптов
docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py

# Проверка синтаксиса
docker exec airflow_webserver python -m py_compile /opt/airflow/scripts/*.py

```
#### Работа с базой данных
```bash
# Подключение к PostgreSQL
docker exec -it weather_postgres psql -U postgres -d weather_db

# Выполнение SQL запроса
docker exec -it weather_postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_data.historical_weather;"

# Экспорт данных
docker exec -t weather_postgres psql -U postgres -d weather_db -c "COPY weather_data.historical_weather TO STDOUT WITH CSV HEADER" > data.csv

# Импорт данных
cat data.csv | docker exec -i weather_postgres psql -U postgres -d weather_db -c "COPY weather_data.historical_weather FROM STDIN CSV HEADER"

```
#### Airflow CLI
```bash
# Управление DAG
docker exec airflow_webserver airflow dags list
docker exec airflow_webserver airflow dags trigger weather_etl_complete
docker exec airflow_webserver airflow dags pause weather_etl_complete
docker exec airflow_webserver airflow dags unpause weather_etl_complete

# Управление задачами
docker exec airflow_webserver airflow tasks list weather_etl_complete
docker exec airflow_webserver airflow tasks test weather_etl_complete forecast_load 2024-01-01

# Информация
docker exec airflow_webserver airflow info
docker exec airflow_webserver airflow version

```
#### Makefile
```bash
# Makefile для автоматизации
.PHONY: help build up down logs clean test

help:
	@echo "Доступные команды:"
	@echo "  make build    - Собрать Docker образы"
	@echo "  make up       - Запустить все сервисы"
	@echo "  make down     - Остановить все сервисы"
	@echo "  make logs     - Показать логи"
	@echo "  make clean    - Очистить все данные"
	@echo "  make test     - Запустить тесты"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "✅ Airflow: http://localhost:8080"
	@echo "✅ Логин: admin, Пароль: admin123"

down:
	docker-compose down

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	docker system prune -f

test:
	docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py
```
## 📈 
### Дополнительные возможности
### Оповещения в Telegram
```bash
# plugins/telegram_alert.py
import requests

def send_telegram_alert(message):
    bot_token = "YOUR_BOT_TOKEN"
    chat_id = "YOUR_CHAT_ID"
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    try:
        response = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        })
        return response.ok
    except Exception as e:
        print(f"Error sending alert: {e}")
        return False

```
### PowerBI интеграция
```bash
# scripts/export_to_powerbi.py
import pandas as pd
from plugins.weather_utils import get_db_engine

def export_for_powerbi():
    engine = get_db_engine()
    query = """
    SELECT 
        timestamp_utc as "Time (UTC)",
        temperature_2m as "Temperature (°C)",
        relative_humidity_2m as "Humidity (%)",
        data_source as "Source"
    FROM weather_data.historical_weather
    WHERE timestamp_utc >= NOW() - INTERVAL '7 days'
    ORDER BY timestamp_utc DESC
    """
    
    df = pd.read_sql(query, engine)
    df.to_csv('weather_data_powerbi.csv', index=False)
    return df
```
### Очистка старых данных
```bash

-- Автоматическая очистка (запускать в DAG)
CREATE OR REPLACE FUNCTION cleanup_old_data()
RETURNS void AS $$
BEGIN
    DELETE FROM weather_data.historical_weather
    WHERE timestamp_utc < NOW() - INTERVAL '30 days';
END;
$$ LANGUAGE plpgsql;

-- Запуск
SELECT cleanup_old_data();
```
### 📄 Лицензия
```bash
MIT License

Copyright (c) 2024 Your Name

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### 🤝 
### Вклад в проект

    *  Fork репозитория

    *  Создайте ветку для ваших изменений

    *  Внесите изменения

    *  Создайте Pull Request

### Руководство по стилю кода

    *  Следуйте PEP 8

    *  Используйте type hints

    *  Добавляйте docstrings

   *   Пишите тесты для нового функционала

## 📞 
### Контакты
*  Contact Sergey Chekryzhov
*  Email sergeyh510@gmail.com
*  GitHub sergeyh510-alt
*  Project Weather-ETL-Pipeline
*  LinkedIn: www.linkedin.com/in/sergey-chekryzhov-a38778217
*  Telegram: @SergeyChekryzhov
