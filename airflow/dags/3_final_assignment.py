from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import pandas as pd
import sqlalchemy
import subprocess

from module import rfm_transform

@dag(start_date=days_ago(1), schedule_interval=None, catchup=False)
def etl_mysql_to_mysql_3():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")
    
    extract = MySqlOperator(
        task_id="extract_from_mysql",
        mysql_conn_id="mysql_default",
        sql="SELECT * FROM final_assignment.online_retail",
        do_xcom_push=True  
    )
    
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=rfm_transform.transform_data
    )

    def load_to_mysql(ti):
        # Pull the transformed data from XCom
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        
        # Create a DataFrame from the transformed data
        df = pd.DataFrame(transformed_data)

        # Establish connection to the target MySQL database
        mysql_conn = BaseHook.get_connection('mysql_default')
        database_name = "final_assignment" 
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{mysql_conn.login}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/{database_name}')
        
        # Load the DataFrame into the target MySQL table
        df.to_sql(name='rfm_segmentation', con=engine, if_exists='replace', index=False)
    
    load = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql
    )
    
    # Define the task dependencies
    start_task >> extract >> transform >> load >> end_task

etl_dag = etl_mysql_to_mysql_3()


