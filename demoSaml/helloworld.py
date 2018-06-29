import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 25),
    'email': ['ashwini.snv@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('tutorial', default_args=default_args, schedule_interval = timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
# t1 = BashOperator(
#     task_id='task_1',
#     bash_command='echo "Hello World from Task 1"',
#     dag=dag)

# t2 = BashOperator(
#     task_id='task_2',
#     bash_command='echo "Hello World from Task 2"',
#     dag=dag)

# t3 = BashOperator(
#     task_id='task_3',
#     bash_command='echo "Hello World from Task 3"',
#     dag=dag)

# t4 = BashOperator(
#     task_id='task_4',
#     bash_command='echo "Hello World from Task 4"',
#     dag=dag)

t1 = BashOperator(
    task_id = 'print_date',
    bash_command = 'date',
    dag = dag)

t2 = BashOperator(
    task_id = 'sleep',
    bash_command = 'sleep 10',
    retries=3,
    dag=dag)

template_command = """ 
{% for i in range(5) %}
  echo "{{ ds }}"
  echo "{{ macros.ds_add(ds,7) }}"
  echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command = template_command,
    params = {'my_param': 'Parameter I passed in'},
    dag = dag)

# t2.set_upstream(t1)
# t3.set_upstream(t1)
# t4.set_upstream(t2)
# t4.set_upstream(t3)

t2.set_upstream(t1)
t3.set_upstream(t1)

