from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone


default_args = {
  'owner': 'ODDS',
}
dag = DAG('dummy_pipeline',
          schedule_interval='*/5 * * * *',
          default_args=default_args,
          start_date=timezone.datetime(2020, 8, 1),
          catchup=False)

# required task id
t1 = DummyOperator(task_id='my_1_dummy_task', dag=dag)
t2 = DummyOperator(task_id='my_2_dummy_task', dag=dag)
t3 = DummyOperator(task_id='my_3_dummy_task', dag=dag)
t4 = DummyOperator(task_id='my_4_dummy_task', dag=dag)
t5 = DummyOperator(task_id='my_5_dummy_task', dag=dag)
t6 = DummyOperator(task_id='my_6_dummy_task', dag=dag)
t7 = DummyOperator(task_id='my_7_dummy_task', dag=dag)
t8 = DummyOperator(task_id='my_8_dummy_task', dag=dag)

t1 >> t2 >> t3 >> t8
t1 >> t4 >> [t5,t6] >> t7 >> t8
