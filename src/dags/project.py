from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pendulum

# ----------------------------
# Настройки
# ----------------------------
LOCAL_DIR = '/data'
GROUP_LOG_FILE = 'group_log.csv'
BASE_URL = 'https://storage.yandexcloud.net/sprint6'
SCHEMA = 'stv2025061616__staging'

# Создаем директорию
os.makedirs(LOCAL_DIR, exist_ok=True)

# Путь к файлу
FILE_PATH = f'{LOCAL_DIR}/{GROUP_LOG_FILE}'

# Параметры подключения к Vertica/PostgreSQL
conn_info = {
    'host': '51.250.75.20',
    'port': 5433,
    'user': 'stv2025061616',
    'password': 'mSWo6LuMXKkA8Uo',
    'database': 'dwh',
    'autocommit': True
}

# ----------------------------
# Функция загрузки в staging
# ----------------------------
def load_to_staging():
    import vertica_python

    sql = f"""
    COPY stv2025061616__staging.group_log (
        group_id,
        user_id,
        user_id_from_str FILLER VARCHAR(50),
        user_id_from AS CASE WHEN user_id_from_str = '' THEN NULL ELSE user_id_from_str::INTEGER END,
        event,
        datetime_orig FILLER VARCHAR(50),
        datetime AS TO_TIMESTAMP(SUBSTR(datetime_orig, 1, 26), 'YYYY-MM-DD HH24:MI:SS.US')
    )
    FROM LOCAL '{FILE_PATH}'
    DELIMITER ',' 
    ENCLOSED BY '"' 
    NULL 'NULL'
    REJECTMAX 1000
    """
    
    try:
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            cursor.execute(sql)
            print(f"✅ Успешно загружено в stv2025061616__staging.group_log")
    except Exception as e:
        raise Exception(f"Ошибка загрузки group_log: {e}")


# Определение DAG
# ----------------------------
@dag(
    schedule_interval=None,
    start_date=pendulum.parse('2025-08-18'),
    catchup=False,
    tags=['sprint6', 'staging', 'group_log']
)
def sprint6_load_group_log_dag1():
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Задача: скачать group_log.csv
    fetch_group_log = BashOperator(
        task_id='fetch_group_log',
        bash_command=f'curl -f -o {FILE_PATH} {BASE_URL}/{GROUP_LOG_FILE}'
    )

    # Задача: загрузить в staging
    load_group_log = PythonOperator(
        task_id='load_group_log_to_staging',
        python_callable=load_to_staging
    )

    # Граф DAG
    start >> fetch_group_log >> load_group_log >> end

# Запуск DAG
_ = sprint6_load_group_log_dag1()