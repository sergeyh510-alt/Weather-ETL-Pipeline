from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_weather_dag',
    default_args=default_args,
    description='Test DAG for Weather ETL',
    schedule_interval='@daily',
    catchup=False,
    tags=['test', 'weather'],
)

def test_connection():
    """Тест подключения к базе данных"""
    import sys
    import os
    sys.path.append('/opt/airflow')

    from plugins.weather_utils import get_db_engine, SCHEMA_NAME, TABLE_NAME
    from sqlalchemy import text

    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}"))
            count = result.scalar()
            logger.info(f"✅ Подключение к БД успешно! Записей в таблице: {count}")
            return f"Success: {count} records"
    except Exception as e:
        logger.error(f"❌ Ошибка подключения: {e}")
        raise

test_task = PythonOperator(
    task_id='test_db_connection',
    python_callable=test_connection,
    dag=dag,
)