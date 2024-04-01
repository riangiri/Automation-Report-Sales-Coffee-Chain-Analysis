'''
=================================================
Milestone 3

Nama  : Raden Rian Girianom
Batch : FTDS-028-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
Adapun dataset yang dipakai mengenai sales coffee chain analysis 2013-2015.
=================================================
'''

# Import Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
#import psycopg2
import psycopg2 as db
import re
import pandas as pd
from elasticsearch import Elasticsearch

def get_data_from_postgres():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)
    df=pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_Raden_Rian_raw.csv')
    conn.close()

def data_cleaning_csv():
    csv_file_path = ('/opt/airflow/dags/P2M3_Raden_Rian_raw.csv')
    dataset = pd.read_csv(csv_file_path)
    
    # drop duplicated value
    dataset.drop_duplicates(inplace=True)

    # Handling missing value dengan impuatation mean jika ada
    for col in dataset.select_dtypes(include=['int64', 'float64']).columns:
        dataset[col].fillna(dataset[col].mean(), inplace=True)

    # rename datatype date dari object menjadi datetime
    if 'date' in dataset.columns:
        dataset['date'] = pd.to_datetime(dataset['date'])

    # rename nama column
    dataset.rename(columns={'differencebetweenactualandtargetprofit': 'difference_target_profit'}, inplace=True)
        
    # lowercase semua nama column
    dataset.columns = dataset.columns.str.lower()      

    # buat replace spasi dengan underscore
    dataset.columns = dataset.columns.str.replace(' ', '_')     
                
    # Save the cleaned data to CSV
    data_csv = dataset.to_csv('/opt/airflow/dags/P2M3_Raden_Rian_clean.csv', index=False)
    
    return data_csv

def upload_to_elasticsearch(): 
    es = Elasticsearch('http://elasticsearch:9200') #define the elasticsearch url
    df=pd.read_csv('/opt/airflow/dags/P2M3_Raden_Rian_clean.csv') 
    for i, r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="postgresm3", doc_type = "doc", body=doc)
        print(res)

default_args = {
    'owner': 'rian',
    'start_date': datetime(2024, 3, 24, 15, 35, 0)-timedelta(hours=7), #dikurangi karena airflow pake utc bukan gmt, 2024, 3(maret), 18(tgl), (jam)13, 56, 0
    'retries': 1, # kalo gagal fail diberi kesempatan cuma 1x habis itu akan ada notif fail jika gagal
    'retry_delay': timedelta(minutes=1),
}

# Define DAG, dan diupdate setiap every 2 minutes
with DAG(
    "RG_Project_M3",
    description='Project Milestone 3 : DAG get data from PostgreSQL, cleaning, and posting to Elasticsearch',
    schedule_interval='30 6 * * *',
    default_args=default_args,
    catchup=False
) as dag:
    
    # Task 1: get data from PostgreSQL
    get_data = PythonOperator(
        task_id='Get_data_task',
        python_callable=get_data_from_postgres,
    )

    # Task for data cleaning
    data_cleaning = PythonOperator(
        task_id='data_cleaning_process',
        python_callable=data_cleaning_csv,
    )

    # Task 3: Transport clean CSV into Elasticsearch
    post_to_elasticsearch = PythonOperator(
        task_id='upload_elasticsearch_task',
        python_callable=upload_to_elasticsearch,
    )
# Define task dependencies
get_data >> data_cleaning >> post_to_elasticsearch