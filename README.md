
<div align="center">

# ⚡ Weather ETL Pipeline with Apache Airflow

<div align="center">
  <a href="./README.md">
    <img src="https://img.shields.io/badge/🇬🇧_English-README-blue?style=for-the-badge&logo=markdown&logoColor=white" alt="English">
  </a>
  <a href="./README.ru.md">
    <img src="https://img.shields.io/badge/🇷🇺_Русский-README-red?style=for-the-badge&logo=markdown&logoColor=white" alt="Русский">
  </a>
</div>

<br><br>

![version](https://img.shields.io/badge/version-1.0.0-blue)
![license](https://img.shields.io/badge/license-MIT-green)
![status](https://img.shields.io/badge/status-active-brightgreen)

</div>

# 📊 Complete Deployment and Usage Guide

## 📋 Table of Contents

   * [Project Description](#project-description)
   * [Architecture](#architecture)
   * [Prerequisites](#prerequisites)
   * [Installation and Setup](#installation-and-setup)
   * [Project Structure](#project-structure)
   * [System Components](#system-components)
   * [Working with DAG](#working-with-dag)
   * [Testing](#testing)
   * [Monitoring and Debugging](#monitoring-and-debugging)
   * [Production Deployment](#production-deployment)
   * [Troubleshooting](#troubleshooting)
   * [Development Commands](#development-commands)
   * [License](#license)
   * [Contacts](#contacts)

 ###  📖 
 ### Project Description

Weather ETL Pipeline is a fully automated system for collecting, processing, and storing weather data using the Open-Meteo API. 
The system is built on Apache Airflow and runs in Docker containers, providing isolation, scalability, and reliability.

### 🎯 Key Features

    ✅ Automatic data collection — loads historical and forecast data every 30 minutes

    ✅ Two loading modes — historical data (on first run) and forecast data (regularly)

    ✅ PostgreSQL storage — all data is stored with UTC timestamps

    ✅ Airflow Web Interface — convenient monitoring and DAG management

    ✅ Docker isolation — all services run in containers

    ✅ Error handling — automatic retries on failures

    ✅ Logging — detailed logs of all operations

### 📊 
### Data Sources

   * Open-Meteo Historical API — archive data from 2020 onwards

   * Open-Meteo Forecast API — 7-day forecast data
### 🗄️ 
### Stored Parameters

* 🌡️ Temperature at 2m

* 💧 Relative Humidity

* 🌡️ Dew Point

* 🌡️ Apparent Temperature

* 🌧️ Precipitation, Rain, Snow

* ❄️ Snow Depth

* 🏷️ Weather Code

* 📊 Pressure (MSL and Surface)

* ☁️ Cloud Cover (Total, Low, Mid, High)

* 💨 Wind (Speed at 10m and 100m, Direction, Gusts)

* 🌱 Soil Parameters (Temperature and Moisture at different depths)

## 🏗️ 
### Architecture
### System Diagram 
<img width="1357" height="893" alt="image" src="https://github.com/user-attachments/assets/4323d2e3-1ae0-446f-83e3-ee16e4b74419" />

     
## Data Flow

<img width="937" height="897" alt="image" src="https://github.com/user-attachments/assets/23fd91bb-8804-4355-9e15-0eb7ddb05ed9" />


## 📋 
### Prerequisites
### System Requirements
|Component         |	Minimum Version	|Recommended Version
|------------------|-----------------------|--------------------|
Docker	           |20.10.x	              |24.0.x+
Docker Compose	   |2.0.x               	|2.20.x+
Python	           |3.8	                  |3.10+
RAM	               |4 GB                  |8 GB+
Disk Space	       |10 GB	                |20 GB+

### Supported OS

  * ✅ Windows 10/11 (с WSL2)

  * ✅ macOS 10.15+

  * ✅ Linux (Ubuntu 20.04+, Debian 11+, CentOS 8+)

### Installing Docker
* Windows (with WSL2)
 powershell

 1. Install WSL2
```bash 
wsl --install


 2. Restart your computer

 3. Download and install Docker Desktop

 https://www.docker.com/products/docker-desktop/

 4. Enable WSL2 in Docker Desktop settings

 Settings → General → Use WSL 2 based engine ✅
```
### macOS
```bash


 1. Download and install Docker Desktop
 https://www.docker.com/products/docker-desktop/

 2. Or via Homebrew
brew install --cask docker
```
### Linux (Ubuntu/Debian)
```bach 


1. Update the system
sudo apt update && sudo apt upgrade -y

 2. Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

 3. Add user to docker group
sudo usermod -aG docker $USER

 4. Reboot or run
newgrp docker

 5. Install Docker Compose Plugin
sudo apt install docker-compose-plugin -y

### Verify Installation
```bash 

 Check Docker
docker --version
 Output: Docker version 24.0.7, build afdd53b

 Check Docker Compose
docker-compose --version
 Output: Docker Compose version v2.23.0

 Check that Docker is running
docker run hello-world
```
## 📂 
## Project Structure
```bach
Mteo_Weather_AirFlow/
│
├── dags/                                    # Airflow DAG files
│   ├── __init__.py
│   ├── weather_etl_complete.py             # Main DAG (159 lines)
│   └── test_dag.py                         # Test DAG
│
├── scripts/                                 # Python ETL scripts
│   ├── __init__.py
│   ├── historical_weather_loader.py       # History loader (~450 lines)
│   └── forecast_weather_loader.py         # Forecast loader (~450 lines)
│
├── plugins/                                 # Airflow plugins
│   ├── __init__.py
│   └── weather_utils.py                   # DB utilities
│
├── docker/                                  # Docker configuration
│   └── requirements.txt                    # Python dependencies
│
├── init-scripts/                            # DB initialization scripts
│   └── 01_create_weather_table.sql        # Table creation
│
├── logs/                                    # Airflow logs
├── data/                                    # Data (API cache)
├── .env                                     # Environment variables
├── .gitignore                               # Git ignore
├── docker-compose.yml                       # Docker Compose configuration
├── Dockerfile                               # Airflow Docker image
├── Makefile                                 # Command automation
└── README.md                                # Documentation
```
###🔧 
### Installation and Setup

#### 1. Clone the repository
```bash


SSH
git clone git@github.com:yourusername/Mteo_Weather_AirFlow.git

HTTPS
git clone https://github.com/yourusername/Mteo_Weather_AirFlow.git

 Navigate to project
cd Mteo_Weather_AirFlow
```
#### 2. Create configuration files
#### .env file
```bash


Create .env file
cat > .env << EOF
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
WEBSERVER_SECRET_KEY=your_super_secret_key_change_me
AIRFLOW_UID=50000
AIRFLOW_GID=50000
EOFOW_GID=50000
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
#### 3. Create SQL table
```bash
init-scripts/01_create_weather_table.sql
sql

-- Create schema
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
    IS 'Table with historical and forecast weather data';

COMMENT ON COLUMN weather_data.historical_weather.timestamp_utc
    IS 'Time in UTC (without offset)';

COMMENT ON COLUMN weather_data.historical_weather.temperature_2m
    IS 'Temperature at 2m in °C';

COMMENT ON COLUMN weather_data.historical_weather.relative_humidity_2m
    IS 'Relative humidity at 2m in %';
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
      - "5434:5432"  # Use free port 5434
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
      - "5435:5432"  # Use free port 5435
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
### Running the Project
#### Quick Start
```bash
 1. Build images
docker-compose build

 2. Start all services
docker-compose up -d

 3. Check status
docker-compose ps

 4. View logs
docker-compose logs -f
```
####  Step-by-Step Launch
```bash
 1. Verify Docker
docker --version
docker-compose --version

 2. Create necessary folders
mkdir -p dags scripts plugins logs init-scripts docker

 3. Copy files to container (if needed)
docker cp dags/weather_etl_complete.py airflow:/opt/airflow/dags/
docker cp scripts/historical_weather_loader.py airflow:/opt/airflow/scripts/
docker cp scripts/forecast_weather_loader.py airflow:/opt/airflow/scripts/
docker cp plugins/weather_utils.py airflow:/opt/airflow/plugins/

 4. Restart Airflow
docker restart airflow_webserver
docker restart airflow_scheduler

 5. Check DAG operation
docker exec airflow_webserver airflow dags list | grep weather
```
#### Verify Operation
```bash
 1. Check PostgreSQL
docker exec -it weather_postgres psql -U postgres -d weather_db -c "\dt weather_data.*"

 2. Check Redis
docker exec -it airflow_redis redis-cli ping

 3. Check Airflow
curl http://localhost:8080/health

 4. Open Airflow UI
 Open browser: http://localhost:8080
 Login: admin, Password: admin123
```

## 📊 
## System Components
### 1. DAG (weather_etl_complete.py)

#### Location: dags/weather_etl_complete.py (159 lines)

#### Description:
Main orchestrator managing the ETL process.

#### Structure:
```bash
 1. Imports (10 lines)
 2. DAG settings (20 lines)  
 3. Wrapper functions (40 lines)
 4. Task definitions (30 lines)
 5. Dependencies (10 lines)
```
#### Logic:
```bash
def check_and_choose_branch():
    """Checks the table and chooses a branch"""
    if table is empty:
        return 'historical_load'    # Load history
    else:
        return 'forecast_load'      # Load forecast


```
#### Dependency Graph:
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

#### Location:
scripts/historical_weather_loader.py (~450 строк)

#### What it does:

* Loads archive data for the last 30 days

* Checks which days are already loaded

* Saves data day by day to PostgreSQL

#### Main functions:
```bash
def get_existing_dates():
    """Get already loaded dates"""
    # Returns a list of dates already present in the DB

def get_historical_data_for_date(date_str):
    """Load data for a specific day"""
    # Request to Open-Meteo Archive API

def save_to_postgres(df):
    """Save data to PostgreSQL"""
    # Insert data with type checking
```
### 3. Forecast Loader (forecast_weather_loader.py)

#### Location: 
scripts/forecast_weather_loader.py (~450 строк)

#### What it does:

* Loads forecast data for today

* Adds only new records (no duplicates)

* Saves to PostgreSQL

#### Features

* Uses requests instead of openmeteo_requests for stability

* Checks for duplicates before insertion

* Handles API errors

### 4. Weather Utils (weather_utils.py)

#### Location: 
plugins/weather_utils.py (~80 строк)

#### Description: 
Utilities for database operations.

#### Functions:
```bash
def get_db_engine():
    """Create DB connection"""
    # Returns SQLAlchemy engine

def check_table_empty():
    """Check if table is empty"""
    # Returns True/False

def get_last_timestamp():
    """Get last timestamp"""
    # Returns datetime

def get_table_stats():
    """Get table statistics"""
    # Returns dictionary with stats
```
### 🔄 
### Working with DAG
#### Triggering DAG

#### Via Web UI:

* Open http://localhost:8080

* Login: admin / admin123

* Find DAG weather_etl_complete

* Toggle ON

* Click ▶️ Trigger DAG

#### ia CLI:
```bash
# Trigger DAG
docker exec airflow_webserver airflow dags trigger weather_etl_complete

# View status
docker exec airflow_webserver airflow dags state weather_etl_complete

# List DAGs
docker exec airflow_webserver airflow dags list
docker exec airflow_webserver airflow dags list
```
### Monitoring Execution

#### Web UI:

* Grid View — table of all runs

* Graph View — visualization of dependencies

* Tree View — hierarchical view

* Log — logs for each task

#### CLI:
```bash
# View logs for a specific task
docker exec airflow_webserver airflow tasks logs \
    weather_etl_complete forecast_load \
    2024-01-01T00:00:00+00:00

# Check task statuses
docker exec airflow_webserver airflow tasks states-for-dag-run \
    weather_etl_complete manual__2024-01-01T00:00:00+00:00


```
#### Scheduling Settings
```bash
# Every 30 minutes
schedule_interval='*/30 * * * *'

# Every hour
schedule_interval='@hourly'

# Every day at midnight
schedule_interval='@daily'

# Every Sunday at 00:00
schedule_interval='0 0 * * 0'

# Cron schedule
schedule_interval='0 0,12 * * *'  # Twice a day
```
### 🧪 
### Testing
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
        print(f"✅ API working! Temperature: {data['current']['temperature_2m']}°C")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == '__main__':
    test_openmeteo_api()
```
### Script Testing
```bash
# Test historical loader
docker exec airflow_webserver python /opt/airflow/scripts/historical_weather_loader.py

# Test forecast loader
docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py

# Test with full logging
docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py 2>&1 | tee /tmp/test.log
```
### Database Testing
```bash
-- Check table structure
\d weather_data.historical_weather

-- Check data
SELECT 
    COUNT(*) as total,
    data_source,
    MIN(timestamp_utc) as first_date,
    MAX(timestamp_utc) as last_date
FROM weather_data.historical_weather
GROUP BY data_source;

-- Check recent records
SELECT 
    timestamp_utc,
    temperature_2m,
    relative_humidity_2m,
    data_source
FROM weather_data.historical_weather 
ORDER BY timestamp_utc DESC 
LIMIT 10;

-- Check duplicates
SELECT 
    timestamp_utc, 
    COUNT(*) 
FROM weather_data.historical_weather
GROUP BY timestamp_utc
HAVING COUNT(*) > 1;
```

### Integration Tests
```bash
# 1. Run DAG in test mode
docker exec airflow_webserver airflow dags test weather_etl_complete 2024-01-01

# 2. Test specific task
docker exec airflow_webserver airflow tasks test \
    weather_etl_complete forecast_load 2024-01-01

# 3. View test logs
docker exec airflow_webserver airflow tasks logs \
    weather_etl_complete forecast_load 2024-01-01
```
## 📊 
## Monitoring and Debugging
### Logs
```bash
# Logs of all containers
docker-compose logs -f

# Logs of specific container
docker logs -f airflow_webserver
docker logs -f airflow_scheduler
docker logs -f weather_postgres

# Logs filtered by errors
docker logs airflow_webserver 2>&1 | grep -i error
docker logs airflow_webserver 2>&1 | grep -i traceback

# Last N lines
docker logs airflow_webserver --tail 100
```
### Metrics and Monitoring
```bash
-- Record count by day
SELECT 
    DATE(timestamp_utc) as date,
    COUNT(*) as records,
    ROUND(AVG(temperature_2m)::numeric, 1) as avg_temp
FROM weather_data.historical_weather
GROUP BY DATE(timestamp_utc)
ORDER BY date DESC;

-- Statistics by data source
SELECT 
    data_source,
    COUNT(*) as total,
    MIN(timestamp_utc) as first_date,
    MAX(timestamp_utc) as last_date
FROM weather_data.historical_weather
GROUP BY data_source;

-- Check for missing data
SELECT 
    DATE(timestamp_utc) as date,
    COUNT(*) as hours,
    CASE 
        WHEN COUNT(*) < 24 THEN '⚠️ Incomplete data'
        ELSE '✅ Complete data'
    END as status
FROM weather_data.historical_weather
GROUP BY DATE(timestamp_utc)
ORDER BY date DESC
LIMIT 7;
```
### DAG Debugging
```bash
# Add debug messages
def load_forecast():
    logger.info("🚀 Starting forecast load")
    try:
        from scripts.forecast_weather_loader import main as forecast_main
        logger.info("📦 Module imported")
        result = forecast_main()
        logger.info(f"📊 Result: {result}")
        return result
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
```

### 🏭 
### Production Deployment
#### Production Settings
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

### Security
```bash
# Generate secure keys
openssl rand -base64 32  # FERNET_KEY
openssl rand -hex 32     # SECRET_KEY
--------------
# Configure .env
cat > .env << EOF
FERNET_KEY=your_key
WEBSERVER_SECRET_KEY=your_secret_key
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=weather_db
EOF
--------------
# Restrict permissions
chmod 600 .env
chmod 644 docker-compose.yml
```
### Scaling
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
### Troubleshooting
#### Common Issues and Solutions
#### 1. Containers don't start
```bash
# Check logs
docker-compose logs

# Restart with cleanup
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d

# Check ports
netstat -ano | findstr :5434  # Windows
sudo lsof -i :5434             # Mac/Linux
```
#### 2. Database connection error
```bash
# Check PostgreSQL status
docker ps | grep postgres

# Test connection
docker exec -it weather_postgres psql -U postgres -d weather_db -c "SELECT 1;"

# Restart PostgreSQL
docker-compose restart weather_postgres
```
#### 3. ModuleNotFoundError
```bash
# Check installed packages
docker exec airflow_webserver pip list

# Install packages
docker exec airflow_webserver pip install pandas numpy sqlalchemy

# Check imports
docker exec airflow_webserver python -c "import pandas; print('OK')"
```
#### 4. API timeout
```bash
# Check API access
docker exec airflow_webserver curl -I https://api.open-meteo.com/v1/forecast

# Check DNS
docker exec airflow_webserver nslookup api.open-meteo.com

# Increase timeout in script
params = {'timeout': 60}  # instead of 30
```

#### 5. DAG not appearing in UI
```bash
# Check syntax
docker exec airflow_webserver python -m py_compile /opt/airflow/dags/weather_etl_complete.py

# Check logs
docker logs airflow_webserver | grep -i "weather_etl_complete"

# Force DAG reload
docker exec airflow_webserver airflow dags list | grep weather
```
#### Data Recovery
```bash
# Database backup
docker exec -t weather_postgres pg_dump -U postgres weather_db > backup.sql

# Restore
cat backup.sql | docker exec -i weather_postgres psql -U postgres weather_db

# Clear table
docker exec -it weather_postgres psql -U postgres -d weather_db -c "
TRUNCATE TABLE weather_data.historical_weather CASCADE;
"
```
### 📝 
### Development Commands
#### Working with Docker
```bash


# Container management
docker-compose up -d          # Start
docker-compose down           # Stop
docker-compose restart        # Restart
docker-compose ps             # Status

# Working with images
docker-compose build          # Build
docker-compose build --no-cache  # Build without cache
docker rmi $(docker images -q)  # Remove all images

# Working with volumes
docker volume ls
docker volume prune -f
docker-compose down -v        # Remove volumes

# Logs
docker-compose logs -f        # All logs
docker-compose logs airflow_webserver -f  # Specific service logs


```
#### Working with Scripts
```bash
# Copy files
docker cp dags/weather_etl_complete.py airflow_webserver:/opt/airflow/dags/
docker cp scripts/forecast_weather_loader.py airflow_webserver:/opt/airflow/scripts/

# Run scripts
docker exec airflow_webserver python /opt/airflow/scripts/forecast_weather_loader.py

# Syntax check
docker exec airflow_webserver python -m py_compile /opt/airflow/scripts/*.py


```
#### Working with Database
```bash
# Connect to PostgreSQL
docker exec -it weather_postgres psql -U postgres -d weather_db

# Execute SQL query
docker exec -it weather_postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_data.historical_weather;"

# Export data
docker exec -t weather_postgres psql -U postgres -d weather_db -c "COPY weather_data.historical_weather TO STDOUT WITH CSV HEADER" > data.csv

# Import data
cat data.csv | docker exec -i weather_postgres psql -U postgres -d weather_db -c "COPY weather_data.historical_weather FROM STDIN CSV HEADER"


```
#### Airflow CLI
```bash
# DAG management
docker exec airflow_webserver airflow dags list
docker exec airflow_webserver airflow dags trigger weather_etl_complete
docker exec airflow_webserver airflow dags pause weather_etl_complete
docker exec airflow_webserver airflow dags unpause weather_etl_complete

# Task management
docker exec airflow_webserver airflow tasks list weather_etl_complete
docker exec airflow_webserver airflow tasks test weather_etl_complete forecast_load 2024-01-01

# Info
docker exec airflow_webserver airflow info
docker exec airflow_webserver airflow version


```
#### Makefile
```bash
# Makefile for automation
.PHONY: help build up down logs clean test

help:
	@echo "Available commands:"
	@echo "  make build    - Build Docker images"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - Show logs"
	@echo "  make clean    - Clean all data"
	@echo "  make test     - Run tests"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "✅ Airflow: http://localhost:8080"
	@echo "✅ Login: admin, Password: admin123"

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
### Additional Features
### Telegram Alerts
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
### PowerBI Integration
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
### Cleaning Old Dataх
```bash


-- Automatic cleanup (run in DAG)
CREATE OR REPLACE FUNCTION cleanup_old_data()
RETURNS void AS $$
BEGIN
    DELETE FROM weather_data.historical_weather
    WHERE timestamp_utc < NOW() - INTERVAL '30 days';
END;
$$ LANGUAGE plpgsql;

-- Run
SELECT cleanup_old_data();
```
### 📄 
### License
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
### Contributing

* Fork the repository

* Create a branch for your changes

* Make your changes

* Create a Pull Request

### Code Style Guide

* Follow PEP 8

* Use type hints

* Add docstrings

* Write tests for new functionality

## 📞 
### Contacts
*  Contact Sergey Chekryzhov
*  Email sergeyh510@gmail.com
*  GitHub sergeyh510-alt
*  Project Weather-ETL-Pipeline
*  LinkedIn: www.linkedin.com/in/sergey-chekryzhov-a38778217
*  Telegram: @SergeyChekryzhov
