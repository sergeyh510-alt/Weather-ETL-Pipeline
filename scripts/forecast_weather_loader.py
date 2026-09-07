# scripts/forecast_weather_loader.py (добавлены load_start_date и load_end_date)
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
TIMEZONE_NAME = 'Europe/Moscow'

def get_forecast_data():
    """Получение прогнозных данных со ВСЕМИ полями"""
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        'latitude': LATITUDE,
        'longitude': LONGITUDE,
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
        'current': [
            'temperature_2m',
            'relative_humidity_2m',
            'apparent_temperature',
            'precipitation',
            'rain',
            'snowfall',
            'weather_code',
            'cloud_cover',
            'pressure_msl',
            'surface_pressure',
            'wind_speed_10m',
            'wind_direction_10m'
        ],
        'forecast_days': 1,
        'timezone': TIMEZONE_NAME
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        logger.info(f"✅ API ответ получен")

        # Парсим часовые данные
        hourly = data.get('hourly', {})
        timestamps_str = hourly.get('time', [])

        if not timestamps_str:
            logger.warning("⚠️ Нет часовых данных в ответе")
            return None

        timestamps = pd.to_datetime(timestamps_str, utc=True)

        # Создаем DataFrame со всеми полями
        df = pd.DataFrame({
            'timestamp_utc': timestamps,
            'temperature_2m': hourly.get('temperature_2m', [np.nan] * len(timestamps)),
            'relative_humidity_2m': hourly.get('relative_humidity_2m', [np.nan] * len(timestamps)),
            'dew_point_2m': hourly.get('dew_point_2m', [np.nan] * len(timestamps)),
            'apparent_temperature': hourly.get('apparent_temperature', [np.nan] * len(timestamps)),
            'precipitation': hourly.get('precipitation', [np.nan] * len(timestamps)),
            'rain': hourly.get('rain', [np.nan] * len(timestamps)),
            'snowfall': hourly.get('snowfall', [np.nan] * len(timestamps)),
            'snow_depth': hourly.get('snow_depth', [np.nan] * len(timestamps)),
            'weather_code': hourly.get('weather_code', [np.nan] * len(timestamps)),
            'pressure_msl': hourly.get('pressure_msl', [np.nan] * len(timestamps)),
            'surface_pressure': hourly.get('surface_pressure', [np.nan] * len(timestamps)),
            'cloud_cover': hourly.get('cloud_cover', [np.nan] * len(timestamps)),
            'cloud_cover_low': hourly.get('cloud_cover_low', [np.nan] * len(timestamps)),
            'cloud_cover_mid': hourly.get('cloud_cover_mid', [np.nan] * len(timestamps)),
            'cloud_cover_high': hourly.get('cloud_cover_high', [np.nan] * len(timestamps)),
            'et0_fao_evapotranspiration': hourly.get('et0_fao_evapotranspiration', [np.nan] * len(timestamps)),
            'vapour_pressure_deficit': hourly.get('vapour_pressure_deficit', [np.nan] * len(timestamps)),
            'wind_speed_10m': hourly.get('wind_speed_10m', [np.nan] * len(timestamps)),
            'wind_speed_100m': hourly.get('wind_speed_100m', [np.nan] * len(timestamps)),
            'wind_direction_10m': hourly.get('wind_direction_10m', [np.nan] * len(timestamps)),
            'wind_direction_100m': hourly.get('wind_direction_100m', [np.nan] * len(timestamps)),
            'wind_gusts_10m': hourly.get('wind_gusts_10m', [np.nan] * len(timestamps)),
            'soil_temperature_0_to_7cm': hourly.get('soil_temperature_0_to_7cm', [np.nan] * len(timestamps)),
            'soil_temperature_7_to_28cm': hourly.get('soil_temperature_7_to_28cm', [np.nan] * len(timestamps)),
            'soil_temperature_28_to_100cm': hourly.get('soil_temperature_28_to_100cm', [np.nan] * len(timestamps)),
            'soil_temperature_100_to_255cm': hourly.get('soil_temperature_100_to_255cm', [np.nan] * len(timestamps)),
            'soil_moisture_0_to_7cm': hourly.get('soil_moisture_0_to_7cm', [np.nan] * len(timestamps)),
            'soil_moisture_7_to_28cm': hourly.get('soil_moisture_7_to_28cm', [np.nan] * len(timestamps)),
            'soil_moisture_28_to_100cm': hourly.get('soil_moisture_28_to_100cm', [np.nan] * len(timestamps)),
            'soil_moisture_100_to_255cm': hourly.get('soil_moisture_100_to_255cm', [np.nan] * len(timestamps))
        })

        # Добавляем текущие данные
        current = data.get('current', {})
        if current and current.get('time'):
            current_time = pd.to_datetime(current.get('time'), utc=True)
            df_current = pd.DataFrame([{
                'timestamp_utc': current_time,
                'temperature_2m': current.get('temperature_2m'),
                'relative_humidity_2m': current.get('relative_humidity_2m'),
                'apparent_temperature': current.get('apparent_temperature'),
                'precipitation': current.get('precipitation'),
                'rain': current.get('rain'),
                'snowfall': current.get('snowfall'),
                'weather_code': current.get('weather_code'),
                'cloud_cover': current.get('cloud_cover'),
                'pressure_msl': current.get('pressure_msl'),
                'surface_pressure': current.get('surface_pressure'),
                'wind_speed_10m': current.get('wind_speed_10m'),
                'wind_direction_10m': current.get('wind_direction_10m')
            }])
            df = pd.concat([df_current, df], ignore_index=True)

        # Удаляем дубликаты и сортируем
        df = df.drop_duplicates(subset=['timestamp_utc'], keep='first')
        df = df.sort_values('timestamp_utc')

        # Оставляем данные за последние 24 часа
        now = pd.Timestamp.now(tz='UTC')
        time_threshold = now - pd.Timedelta(hours=24)
        future_threshold = now + pd.Timedelta(hours=3)

        df_filtered = df[
            (df['timestamp_utc'] >= time_threshold) &
            (df['timestamp_utc'] <= future_threshold)
            ].copy()

        logger.info(f"📊 Всего {len(df)} записей, за последние 24 часа: {len(df_filtered)}")

        if len(df_filtered) > 0:
            logger.info(f"🕐 Диапазон: {df_filtered['timestamp_utc'].min()} - {df_filtered['timestamp_utc'].max()}")

        return df_filtered

    except Exception as e:
        logger.error(f"❌ Ошибка API: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_existing_timestamps():
    """Получение существующих временных меток из БД"""
    try:
        from plugins.weather_utils import get_db_engine, SCHEMA_NAME, TABLE_NAME
        engine = get_db_engine()
        with engine.connect() as conn:
            query = text(f"""
                SELECT DISTINCT timestamp_utc 
                FROM {SCHEMA_NAME}.{TABLE_NAME}
                WHERE latitude = :lat AND longitude = :lon
            """)
            result = conn.execute(query, {"lat": LATITUDE, "lon": LONGITUDE})
            existing = set()
            for row in result:
                ts = row[0]
                if isinstance(ts, datetime):
                    existing.add(ts.replace(microsecond=0))
            logger.info(f"📊 Найдено {len(existing)} существующих записей")
            return existing
    except Exception as e:
        logger.error(f"❌ Ошибка получения существующих записей: {e}")
        return set()

def save_to_postgres(df):
    """Сохранение прогнозных данных со всеми полями"""
    if df is None or len(df) == 0:
        return False

    try:
        from plugins.weather_utils import get_db_engine, SCHEMA_NAME, TABLE_NAME
        from sqlalchemy.types import TIMESTAMP, Integer, Float, String, Date

        # Получаем существующие записи
        existing_timestamps = get_existing_timestamps()

        # Фильтруем только новые записи
        def is_new(row):
            ts = row['timestamp_utc']
            if isinstance(ts, pd.Timestamp):
                ts = ts.to_pydatetime().replace(microsecond=0)
            elif isinstance(ts, datetime):
                ts = ts.replace(microsecond=0)
            return ts not in existing_timestamps

        df_new = df[df.apply(is_new, axis=1)].copy()

        logger.info(f"📊 Всего {len(df)} записей, новых: {len(df_new)}")

        if len(df_new) == 0:
            logger.info('Нет новых данных для сохранения')
            return True

        # ✅ ИСПРАВЛЕНО: Добавляем даты загрузки
        now = datetime.now(pytz.UTC)
        load_date = now.strftime('%Y-%m-%d')

        # Добавляем метаданные
        df_new['timezone_offset'] = '+3'
        df_new['latitude'] = LATITUDE
        df_new['longitude'] = LONGITUDE
        df_new['location_name'] = LOCATION_NAME
        df_new['data_source'] = 'Open-Meteo Forecast API'
        df_new['load_start_date'] = load_date
        df_new['load_end_date'] = load_date

        # Временные признаки
        df_new['hour_utc'] = df_new['timestamp_utc'].dt.hour
        df_new['day_of_week_utc'] = df_new['timestamp_utc'].dt.dayofweek
        df_new['month_utc'] = df_new['timestamp_utc'].dt.month
        df_new['year_utc'] = df_new['timestamp_utc'].dt.year

        # Преобразуем timestamp в строку
        df_new['timestamp_utc'] = df_new['timestamp_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')

        engine = get_db_engine()

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

        existing_columns = [col for col in columns_to_insert if col in df_new.columns]
        df_to_insert = df_new[existing_columns]
        final_dtype = {col: dtype_mapping[col] for col in existing_columns if col in dtype_mapping}

        logger.info(f'📤 Вставка {len(df_to_insert)} записей...')
        logger.info(f'📋 Колонки: {existing_columns}')

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

        # Проверяем результат
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}'))
            count = result.scalar()
            logger.info(f'📊 В таблице теперь {count} записей')

        logger.info('✅ Прогнозные данные успешно загружены!')
        return True

    except Exception as e:
        logger.error(f'❌ Ошибка сохранения: {e}')
        import traceback
        traceback.print_exc()
        return False

def main():
    """Главная функция"""
    logger.info('=' * 60)
    logger.info('🌤️ ЗАГРУЗКА ПРОГНОЗНЫХ ДАННЫХ')
    logger.info('=' * 60)

    try:
        df = get_forecast_data()

        if df is None or len(df) == 0:
            logger.info('Нет данных для сохранения')
            return True

        return save_to_postgres(df)

    except Exception as e:
        logger.error(f'❌ Ошибка: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    main()