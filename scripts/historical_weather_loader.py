# scripts/historical_weather_loader.py
import sys
import os
sys.path.append('/opt/airflow')

import logging
from datetime import datetime, timedelta
import pytz
import requests
import pandas as pd
import numpy as np
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LATITUDE = 55.5764
LONGITUDE = 37.4365
LOCATION_NAME = 'Moscow'
TIMEZONE_OFFSET_HOURS = '+3'

# ===== НАСТРОЙКИ ДИАПАЗОНА ЗАГРУЗКИ =====
# Измените эти даты для загрузки нужного периода
START_DATE = '2020-01-01'  # Начало загрузки (2020 год)
END_DATE = '2026-09-05'    # Конец загрузки (текущая дата)
# ==========================================

def get_historical_data_for_date(date_str):
    """Получение исторических данных за один день через requests"""
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        'latitude': LATITUDE,
        'longitude': LONGITUDE,
        'start_date': date_str,
        'end_date': date_str,
        'hourly': [
            'temperature_2m',
            'relative_humidity_2m',
            'dew_point_2m',
            'apparent_temperature',
            'precipitation',
            'rain',
            'snowfall',
            'snow_depth',
            'weather_code',
            'pressure_msl',
            'surface_pressure',
            'cloud_cover',
            'cloud_cover_low',
            'cloud_cover_mid',
            'cloud_cover_high',
            'et0_fao_evapotranspiration',
            'vapour_pressure_deficit',
            'wind_speed_10m',
            'wind_speed_100m',
            'wind_direction_10m',
            'wind_direction_100m',
            'wind_gusts_10m',
            'soil_temperature_0_to_7cm',
            'soil_temperature_7_to_28cm',
            'soil_temperature_28_to_100cm',
            'soil_temperature_100_to_255cm',
            'soil_moisture_0_to_7cm',
            'soil_moisture_7_to_28cm',
            'soil_moisture_28_to_100cm',
            'soil_moisture_100_to_255cm'
        ],
        'timezone': 'UTC'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        hourly = data.get('hourly', {})
        timestamps = pd.to_datetime(hourly.get('time', []))

        # Создаем DataFrame со всеми полями
        df = pd.DataFrame({
            'timestamp_utc': timestamps,
            'temperature_2m': hourly.get('temperature_2m', []),
            'relative_humidity_2m': hourly.get('relative_humidity_2m', []),
            'dew_point_2m': hourly.get('dew_point_2m', []),
            'apparent_temperature': hourly.get('apparent_temperature', []),
            'precipitation': hourly.get('precipitation', []),
            'rain': hourly.get('rain', []),
            'snowfall': hourly.get('snowfall', []),
            'snow_depth': hourly.get('snow_depth', []),
            'weather_code': hourly.get('weather_code', []),
            'pressure_msl': hourly.get('pressure_msl', []),
            'surface_pressure': hourly.get('surface_pressure', []),
            'cloud_cover': hourly.get('cloud_cover', []),
            'cloud_cover_low': hourly.get('cloud_cover_low', []),
            'cloud_cover_mid': hourly.get('cloud_cover_mid', []),
            'cloud_cover_high': hourly.get('cloud_cover_high', []),
            'et0_fao_evapotranspiration': hourly.get('et0_fao_evapotranspiration', []),
            'vapour_pressure_deficit': hourly.get('vapour_pressure_deficit', []),
            'wind_speed_10m': hourly.get('wind_speed_10m', []),
            'wind_speed_100m': hourly.get('wind_speed_100m', []),
            'wind_direction_10m': hourly.get('wind_direction_10m', []),
            'wind_direction_100m': hourly.get('wind_direction_100m', []),
            'wind_gusts_10m': hourly.get('wind_gusts_10m', []),
            'soil_temperature_0_to_7cm': hourly.get('soil_temperature_0_to_7cm', []),
            'soil_temperature_7_to_28cm': hourly.get('soil_temperature_7_to_28cm', []),
            'soil_temperature_28_to_100cm': hourly.get('soil_temperature_28_to_100cm', []),
            'soil_temperature_100_to_255cm': hourly.get('soil_temperature_100_to_255cm', []),
            'soil_moisture_0_to_7cm': hourly.get('soil_moisture_0_to_7cm', []),
            'soil_moisture_7_to_28cm': hourly.get('soil_moisture_7_to_28cm', []),
            'soil_moisture_28_to_100cm': hourly.get('soil_moisture_28_to_100cm', []),
            'soil_moisture_100_to_255cm': hourly.get('soil_moisture_100_to_255cm', [])
        })

        logger.info(f"✅ Загружено {len(df)} записей за {date_str}")
        return df

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки за {date_str}: {e}")
        return None

def get_existing_dates():
    """Получение уже загруженных дат"""
    try:
        from plugins.weather_utils import get_db_engine, SCHEMA_NAME, TABLE_NAME
        engine = get_db_engine()
        with engine.connect() as conn:
            query = text(f"""
                SELECT DISTINCT DATE(timestamp_utc) as date
                FROM {SCHEMA_NAME}.{TABLE_NAME}
                WHERE latitude = :lat AND longitude = :lon
                ORDER BY date
            """)
            result = conn.execute(query, {"lat": LATITUDE, "lon": LONGITUDE})
            existing_dates = [row[0] for row in result]
            logger.info(f"📊 Найдено {len(existing_dates)} дней в БД")
            return existing_dates
    except Exception as e:
        logger.error(f"❌ Ошибка получения существующих дат: {e}")
        return []

def save_to_postgres(df, load_start_date, load_end_date):
    """Сохранение данных в PostgreSQL со всеми полями"""
    if df is None or len(df) == 0:
        return False

    try:
        from plugins.weather_utils import get_db_engine, SCHEMA_NAME, TABLE_NAME

        # Добавляем метаданные
        df['timezone_offset'] = TIMEZONE_OFFSET_HOURS
        df['latitude'] = LATITUDE
        df['longitude'] = LONGITUDE
        df['location_name'] = LOCATION_NAME
        df['data_source'] = 'Open-Meteo Historical API'

        # ✅ ИСПРАВЛЕНО: Добавляем даты загрузки
        df['load_start_date'] = load_start_date
        df['load_end_date'] = load_end_date

        # Временные признаки
        df['hour_utc'] = df['timestamp_utc'].dt.hour
        df['day_of_week_utc'] = df['timestamp_utc'].dt.dayofweek
        df['month_utc'] = df['timestamp_utc'].dt.month
        df['year_utc'] = df['timestamp_utc'].dt.year

        # Преобразуем timestamp в строку
        df['timestamp_utc'] = df['timestamp_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')

        engine = get_db_engine()

        from sqlalchemy.types import TIMESTAMP, Integer, Float, String, Date

        dtype_mapping = {
            'timestamp_utc': TIMESTAMP(timezone=True),
            'timezone_offset': String(10),
            'temperature_2m': Float(),
            'relative_humidity_2m': Float(),
            'dew_point_2m': Float(),
            'apparent_temperature': Float(),
            'precipitation': Float(),
            'rain': Float(),
            'snowfall': Float(),
            'snow_depth': Float(),
            'weather_code': Integer(),
            'pressure_msl': Float(),
            'surface_pressure': Float(),
            'cloud_cover': Float(),
            'cloud_cover_low': Float(),
            'cloud_cover_mid': Float(),
            'cloud_cover_high': Float(),
            'et0_fao_evapotranspiration': Float(),
            'vapour_pressure_deficit': Float(),
            'wind_speed_10m': Float(),
            'wind_speed_100m': Float(),
            'wind_direction_10m': Float(),
            'wind_direction_100m': Float(),
            'wind_gusts_10m': Float(),
            'soil_temperature_0_to_7cm': Float(),
            'soil_temperature_7_to_28cm': Float(),
            'soil_temperature_28_to_100cm': Float(),
            'soil_temperature_100_to_255cm': Float(),
            'soil_moisture_0_to_7cm': Float(),
            'soil_moisture_7_to_28cm': Float(),
            'soil_moisture_28_to_100cm': Float(),
            'soil_moisture_100_to_255cm': Float(),
            'latitude': Float(),
            'longitude': Float(),
            'location_name': String(100),
            'data_source': String(100),
            'load_start_date': Date(),
            'load_end_date': Date(),
            'hour_utc': Integer(),
            'day_of_week_utc': Integer(),
            'month_utc': Integer(),
            'year_utc': Integer()
        }

        columns_to_insert = [
            'timestamp_utc', 'timezone_offset',
            'temperature_2m', 'relative_humidity_2m', 'dew_point_2m',
            'apparent_temperature', 'precipitation', 'rain', 'snowfall',
            'snow_depth', 'weather_code', 'pressure_msl', 'surface_pressure',
            'cloud_cover', 'cloud_cover_low', 'cloud_cover_mid', 'cloud_cover_high',
            'et0_fao_evapotranspiration', 'vapour_pressure_deficit',
            'wind_speed_10m', 'wind_speed_100m', 'wind_direction_10m',
            'wind_direction_100m', 'wind_gusts_10m',
            'soil_temperature_0_to_7cm', 'soil_temperature_7_to_28cm',
            'soil_temperature_28_to_100cm', 'soil_temperature_100_to_255cm',
            'soil_moisture_0_to_7cm', 'soil_moisture_7_to_28cm',
            'soil_moisture_28_to_100cm', 'soil_moisture_100_to_255cm',
            'latitude', 'longitude', 'location_name', 'data_source',
            'load_start_date', 'load_end_date',
            'hour_utc', 'day_of_week_utc', 'month_utc', 'year_utc'
        ]

        existing_columns = [col for col in columns_to_insert if col in df.columns]
        df_to_insert = df[existing_columns]
        final_dtype = {col: dtype_mapping[col] for col in existing_columns if col in dtype_mapping}

        logger.info(f"📤 Вставка {len(df_to_insert)} записей...")
        logger.info(f"📋 Колонки: {existing_columns}")
        logger.info(f"📅 load_start_date: {load_start_date}")
        logger.info(f"📅 load_end_date: {load_end_date}")

        df_to_insert.to_sql(
            TABLE_NAME,
            engine,
            schema=SCHEMA_NAME,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000,
            dtype=final_dtype
        )

        logger.info("✅ Данные успешно сохранены!")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка сохранения: {e}")
        import traceback
        traceback.print_exc()
        return False

def main(days_back=None):
    """Главная функция - загрузка исторических данных"""
    logger.info("\n" + "█" * 60)
    logger.info("   ЗАГРУЗКА ИСТОРИЧЕСКИХ ПОГОДНЫХ ДАННЫХ")
    logger.info("█" * 60)

    try:
        existing_dates = get_existing_dates()
        existing_dates_set = set(existing_dates)

        # Определяем диапазон загрузки
        if days_back:
            # Если указано количество дней назад (для обратной совместимости)
            end_date = datetime.now(pytz.UTC).date()
            start_date = end_date - timedelta(days=days_back)
            logger.info(f"📅 Режим: загрузка последних {days_back} дней")
        else:
            # Иначе загружаем с START_DATE по END_DATE
            start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
            end_date = datetime.strptime(END_DATE, '%Y-%m-%d').date()
            logger.info(f"📅 Режим: загрузка с {START_DATE} по {END_DATE}")

        # Создаем список дат для загрузки (только те, которых нет в БД)
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str not in existing_dates_set:
                date_list.append(date_str)
            current_date += timedelta(days=1)

        logger.info(f"📅 Найдено {len(date_list)} дней для загрузки")
        logger.info(f"📅 Диапазон: {start_date} - {end_date}")

        if len(date_list) == 0:
            logger.info("✅ Все данные уже загружены")
            return True

        total_success = 0
        total_failed = 0

        # Сохраняем даты загрузки для всех записей
        load_start = start_date.strftime('%Y-%m-%d')
        load_end = end_date.strftime('%Y-%m-%d')

        for date_str in date_list:
            df = get_historical_data_for_date(date_str)
            if df is not None and len(df) > 0:
                # Передаем даты загрузки в функцию сохранения
                if save_to_postgres(df, load_start, load_end):
                    total_success += 1
                    logger.info(f"✅ Загружен день: {date_str}")
                else:
                    total_failed += 1
                    logger.error(f"❌ Ошибка сохранения дня: {date_str}")
            else:
                total_failed += 1
                logger.error(f"❌ Ошибка загрузки дня: {date_str}")

            # Небольшая пауза между запросами, чтобы не перегружать API
            import time
            time.sleep(0.5)

        logger.info(f"✅ Успешно загружено {total_success} из {len(date_list)} дней")
        logger.info(f"❌ Неудачно: {total_failed} дней")

        return total_success == len(date_list)

    except Exception as e:
        logger.error(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()