from datetime import datetime, timedelta
from pprint import pprint
import time


from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG


default_args = {
    'owner': 'Victory Chang',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['victorychang@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='my_tutorial',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# airflow initdb

bash_task = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

bash_task_2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag
)

bash_task_3 = BashOperator(
    task_id='echo_name',
    bash_command='echo Victory',
    dag=dag
)

bash_task >> bash_task_2 >> bash_task_3


def print_context(ds, **kwargs):
    pprint(kwargs)
    return 'Completed print_context (this will be in logs)'


python_task = PythonOperator(
    task_id='print_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag
)

def sleep_function(duration):
    print('sleeping for {} seconds'.format(duration))
    time.sleep(duration)

python_task_2 = PythonOperator(
    task_id='sleep_python',
    python_callable=sleep_function,
    op_kwargs={'duration': 3},
    dag=dag
)

python_task >> python_task_2


dummy_task = DummyOperator(
    task_id='dummy',
    dag=dag
)