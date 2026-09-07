# plugins/weather_utils.py
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)

# Для Docker используем имена сервисов
DB_CONFIG = {
    'host': 'weather_postgres',  # Имя сервиса в docker-compose
    'port': 5432,  # Внутренний порт
    'database': 'weather_db',
    'user': 'postgres',
    'password': '12345'
}

SCHEMA_NAME = 'weather_data'
TABLE_NAME = 'historical_weather'

# Полный список всех колонок таблицы
ALL_COLUMNS = [
    'id', 'timestamp_utc', 'timezone_offset',
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

def get_db_engine():
    """Создание подключения к БД"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    logger.info(f"🔗 Подключение к БД: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    return create_engine(connection_string)

def check_table_empty():
    """Проверка, пустая ли таблица"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}")
            result = conn.execute(query)
            count = result.scalar()
            logger.info(f"📊 В таблице {count} записей")
            return count == 0
    except Exception as e:
        logger.error(f"❌ Ошибка проверки таблицы: {e}")
        return True

def get_last_timestamp():
    """Получение последней временной метки"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = text(f"SELECT MAX(timestamp_utc) FROM {SCHEMA_NAME}.{TABLE_NAME}")
            result = conn.execute(query)
            last_ts = result.scalar()
            if last_ts:
                logger.info(f"🕐 Последняя запись: {last_ts}")
            else:
                logger.info("🕐 В таблице нет записей")
            return last_ts
    except Exception as e:
        logger.error(f"❌ Ошибка получения последней временной метки: {e}")
        return None

def get_table_stats():
    """Получение статистики по таблице"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            count_query = text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}")
            total_count = conn.execute(count_query).scalar()

            range_query = text(f"""
                SELECT MIN(timestamp_utc) as min_date, MAX(timestamp_utc) as max_date
                FROM {SCHEMA_NAME}.{TABLE_NAME}
            """)
            result = conn.execute(range_query)
            row = result.fetchone()

            # Получаем информацию о колонках
            columns_query = text(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """)
            columns_result = conn.execute(columns_query, {"schema": SCHEMA_NAME, "table": TABLE_NAME})
            columns_info = [{"name": row[0], "type": row[1]} for row in columns_result]

            stats = {
                'total_records': total_count,
                'min_date': row[0] if row else None,
                'max_date': row[1] if row else None,
                'columns': columns_info
            }

            logger.info(f"📊 Статистика таблицы: {stats}")
            return stats
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики: {e}")
        return None