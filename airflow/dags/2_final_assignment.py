from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import pandas as pd
import sqlalchemy
import subprocess
import numpy as np


@dag(start_date=days_ago(1), schedule_interval=None, catchup=False)
def etl_mysql_to_mysql_2():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")
    
    extract = MySqlOperator(
        task_id="extract_from_mysql",
        mysql_conn_id="mysql_default",
        sql="SELECT * FROM final_assignment.online_retail",
        do_xcom_push=True  
    )
    
    def install_dependencies():
        subprocess.check_call(['pip', 'install', 'scikit-learn', 'pymysql'])

    install_dependencies_task = PythonOperator(
        task_id='install_dependencies',
        python_callable=install_dependencies
    )

    def transform_data(ti):
        from sklearn.preprocessing import StandardScaler
        from sklearn.cluster import KMeans

        extracted_data = ti.xcom_pull(task_ids='extract_from_mysql')
        columns = ['index', 'InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
        df = pd.DataFrame(extracted_data, columns=columns)

        df['Quantity'] = abs(df['Quantity'])
        df = df[df['UnitPrice'] > 0]
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df['month_index'] = df['InvoiceDate'].dt.month * 0.01 + df['InvoiceDate'].dt.year
        df['revenue'] = df['UnitPrice'] * df['Quantity']

        result = df.groupby(['CustomerID']).agg(
            first_transactions=pd.NamedAgg(column='InvoiceDate', aggfunc='min'),
            last_transactions=pd.NamedAgg(column='InvoiceDate', aggfunc='max'),
            Total_revenue=pd.NamedAgg(column='revenue', aggfunc='sum'),
            Number_Transactions=pd.NamedAgg(column='CustomerID', aggfunc='count'),
            nunique_month_index=pd.NamedAgg(column='month_index', aggfunc=pd.Series.nunique)
        ).reset_index()

        max_date = df['InvoiceDate'].max()
        result['first-last'] =(result['last_transactions'].dt.month +  result['last_transactions'].dt.year *12 ) - (result['first_transactions'].dt.month + result['first_transactions'].dt.year * 12) + 1
        result['User Age'] =(max_date.month +  max_date.year *12 ) - (result['first_transactions'].dt.month + result['first_transactions'].dt.year * 12) + 1
        result['recency'] = result['User Age'] - result['first-last'] + 1

        result['Recency-Log'] = np.log(result['recency'])
        result['Monetary-Log'] = np.log(result['Total_revenue'])
        result['Frequency-Log'] = np.log(result['Number_Transactions'])
        
        max_GTV = max(result['Monetary-Log'])
        min_GTV = min(result['Monetary-Log'])
        max_TRX = max(result['Frequency-Log'])
        min_TRX = min(result['Frequency-Log'])
        min_recency = min(result['Recency-Log'])
        max_recency = max(result['Recency-Log'])

        result['Recency_Score-Log'] = 5 * (result['Recency-Log'] - min_recency) / (max_recency - min_recency)
        result['Frequency_Score-Log'] = 5 * (result['Frequency-Log'] - min_TRX) / (max_TRX - min_TRX)
        result['Monetary_Score-Log'] = 5 * (result['Monetary-Log'] - min_GTV) / (max_GTV - min_GTV)

        result_score = result[['Recency_Score-Log', 'Frequency_Score-Log', 'Monetary_Score-Log']]

        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(result_score)

        k = 4  # Set the number of clusters
        kmeans = KMeans(n_clusters=k, random_state=42).fit(df_scaled)
        result_score['cluster_label'] = kmeans.fit_predict(df_scaled)

        def assign_cluster_label(label):
                if label == 0:
                    return 'Sleep'
                elif label == 1:
                    return 'Potential'
                elif label == 2:
                    return 'Champion'
                elif label == 3:
                    return 'Lost'


        result_score['cluster_label_new'] = result_score['cluster_label'].apply(assign_cluster_label)

        df_final = pd.concat([result, result_score[['cluster_label', 'cluster_label_new']]], axis=1)
        df_final = df_final[['CustomerID', 'Total_revenue', 'Number_Transactions', 'recency', 'cluster_label', 'cluster_label_new']] 
     
        ti.xcom_push(key='transformed_data', value=df_final.to_dict('records'))

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    def load_to_mysql(ti):
        # Pull the transformed data from XCom
        transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        
        # Create a DataFrame from the transformed data
        df = pd.DataFrame(transformed_data)

        # Establish connection to the target MySQL database
        mysql_conn = BaseHook.get_connection('mysql_default')
        database_name = "final_assignment"  # Replace with your actual database name
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{mysql_conn.login}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/{database_name}')
        
        # Load the DataFrame into the target MySQL table
        df.to_sql(name='rfm_segmentation', con=engine, if_exists='replace', index=False)
    
    load = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql
    )
    
    # Define the task dependencies
    start_task >> extract >> install_dependencies_task >> transform >> load >> end_task

etl_dag = etl_mysql_to_mysql_2()


