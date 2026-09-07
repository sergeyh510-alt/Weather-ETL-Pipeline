FROM apache/airflow:2.7.0-python3.10

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Копируем зависимости
COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Создаем директории (уже от пользователя airflow)
RUN mkdir -p /opt/airflow/dags /opt/airflow/scripts /opt/airflow/plugins /opt/airflow/logs

# Копируем код
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY plugins/ /opt/airflow/plugins/

# Устанавливаем права (уже от пользователя airflow)
USER root
RUN chown -R airflow: /opt/airflow/dags /opt/airflow/scripts /opt/airflow/plugins /opt/airflow/logs
USER airflow

# Устанавливаем PYTHONPATH
ENV PYTHONPATH=/opt/airflow