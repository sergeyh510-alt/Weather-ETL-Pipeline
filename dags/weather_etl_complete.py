# dags/weather_etl_complete.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow')

from plugins.weather_utils import check_table_empty, get_table_stats, get_last_timestamp

import logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'weather_etl_complete',
    default_args=default_args,
    description='Полный ETL пайплайн для погодных данных',
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['weather', 'etl', 'complete'],
)

def check_and_choose_branch(**context):
    """Проверяет состояние таблицы и выбирает ветку выполнения"""
    logger.info("=" * 60)
    logger.info("🔍 ПРОВЕРКА СОСТОЯНИЯ ТАБЛИЦЫ")
    logger.info("=" * 60)

    try:
        is_empty = check_table_empty()

        if is_empty:
            logger.info("📭 Таблица пуста → Загрузка исторических данных")
            return 'historical_load'
        else:
            stats = get_table_stats()
            last_ts = get_last_timestamp()
            logger.info(f"📊 Таблица содержит {stats['total_records']} записей")
            logger.info(f"🕐 Последняя запись: {last_ts}")
            logger.info("🔄 Таблица не пуста → Загрузка прогнозных данных")
            return 'forecast_load'
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке: {e}")
        return 'error_task'

def load_historical():
    """Загрузка исторических данных"""
    try:
        from scripts.historical_weather_loader import main as historical_main
        # Загружаем ВСЕ данные (без ограничения по дням)
        result = historical_main()  # Убрал days_back=30
        if result:
            logger.info("✅ Исторические данные загружены успешно!")
        else:
            logger.error("❌ Ошибка загрузки исторических данных!")
        return result
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        raise

def load_forecast():
    """Загрузка прогнозных данных"""
    try:
        from scripts.forecast_weather_loader import main as forecast_main
        result = forecast_main()
        if result:
            logger.info("✅ Прогнозные данные загружены успешно!")
        else:
            logger.error("❌ Ошибка загрузки прогнозных данных!")
        return result
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        raise

def log_complete(**context):
    """Логирование завершения"""
    stats = get_table_stats()
    logger.info("=" * 60)
    logger.info("✅ ETL ПАЙПЛАЙН ЗАВЕРШЕН УСПЕШНО!")
    if stats:
        logger.info(f"📊 Итоговая статистика:")
        logger.info(f"   - Всего записей: {stats.get('total_records', 0)}")
        logger.info(f"   - Диапазон: {stats.get('min_date')} - {stats.get('max_date')}")
    logger.info("=" * 60)
    return "ETL завершен"

def error_handler(**context):
    """Обработчик ошибок"""
    logger.error("=" * 60)
    logger.error("❌ ОШИБКА В ПАЙПЛАЙНЕ!")
    logger.error("Проверьте подключение к базе данных")
    logger.error("=" * 60)
    return "Ошибка выполнения"

# ========== ЗАДАЧИ ==========

branch_task = BranchPythonOperator(
    task_id='branch_check_table',
    python_callable=check_and_choose_branch,
    provide_context=True,
    dag=dag,
)

load_historical_task = PythonOperator(
    task_id='historical_load',
    python_callable=load_historical,
    provide_context=True,
    dag=dag,
)

load_forecast_task = PythonOperator(
    task_id='forecast_load',
    python_callable=load_forecast,
    provide_context=True,
    dag=dag,
)

log_complete_task = PythonOperator(
    task_id='log_complete',
    python_callable=log_complete,
    provide_context=True,
    dag=dag,
)

error_task = PythonOperator(
    task_id='error_task',
    python_callable=error_handler,
    provide_context=True,
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# ========== ✅ ИСПРАВЛЕННЫЕ ЗАВИСИМОСТИ ==========

# 1. Ветвление
branch_task >> [load_historical_task, load_forecast_task, error_task]

# 2. Обе ветки загрузки ведут к log_complete
load_historical_task >> log_complete_task
load_forecast_task >> log_complete_task

# 3. log_complete ведет к end_pipeline
log_complete_task >> end_pipeline

# 4. error_task тоже ведет к end_pipeline (чтобы пайплайн завершался)
error_task >> end_pipeline