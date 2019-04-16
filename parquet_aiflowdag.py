from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.email import send_email
from airflow.operators.email_operator import EmailOperator


default_args = {'owner': 'nikhil', 'depends_on_past': False, 'start_date': datetime(2019, 04, 16)	}
	
dag = DAG('parquetdag',default_args=default_args)

start = DummyOperator(
    task_id='dummy_task',
    dag=dag)


import_parq = BashOperator(
    task_id='import_paraquet',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Nikhil/readparqutespark.py',
    dag=dag)




queryforjson = BashOperator(
    task_id='QueryForJson',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Nikhil/QueryToJson.py',
    dag=dag)    

jsontopara = BashOperator(
    task_id='JsonToParquet',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Nikhil/JsonToParquet.py',
    dag=dag)    



import_parq>>queryforjson>>jsontopara










