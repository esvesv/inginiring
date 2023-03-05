
#
# В этом примере мы постараемся преобразовать наш пример из ноутбука Jupiter в DAG airflow
#

from airflow import DAG
from datetime import datetime

# здесь используемые функциональные блоки

def my_function1:
    return 1


def my_function2:
    return 2


# собственно, структура и последовательность DAG


with DAG(
    dag_id='назови меня как-нибудь',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 3, 3),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
) as dag:

    task1 = PythonOperator (
        task_id = 'task1',
        python_callable = my_function1
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=my_function2
    )

    task1 >> task2