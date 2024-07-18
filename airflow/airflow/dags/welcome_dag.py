import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import requests
import psycopg2
# Параметры DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

dag = DAG(
    'moex_to_postgres',
    #default_args=default_args,
    description='Fetch stock candle data from MOEX and store in PostgreSQL',
    schedule_interval=timedelta(minutes=1),
    start_date=pendulum.datetime(2024, 7, 12),
    catchup = False
)

def get_last_ending_date():
    conn = psycopg2.connect(
        dsn="postgresql://Moexdb_owner:Z6x5RkhAGJQm@ep-old-sound-a2zto35h.eu-central-1.aws.neon.tech/Moexdb?sslmode=require"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT ending FROM pract ORDER BY ending DESC LIMIT 1;")
    last_date = cursor.fetchone()
    cursor.close()
    conn.close()
    if last_date:
        return last_date[0]
    else:
        return '2024-07-12'  



tickers = ['SBER', 'GAZP', 'LKOH']  
interval = 1
# Функция для получения данных от MOEX API
def fetch_moex_data(**context):
    all_prices = []
    start_date = context['task_instance'].xcom_pull(task_ids='get_start_date')
    for ticker in tickers:
        url = f"http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?from={start_date}&interval=1"
        response = requests.get(url)
        data = response.json()
        for candle in data['candles']['data']:
            price = {
                'ticker': ticker,
                'interval': interval,
                'open': candle[0],
                'close': candle[1],
                'high': candle[2],
                'low': candle[3],
                'value': candle[4],
                'volume': candle[5],
                'begin': candle[6],
                'ending': candle[7]
            }
            all_prices.append(price)
    return all_prices

# Функция для загрузки данных в PostgreSQL
def load_to_postgres(**context):
    prices = context['task_instance'].xcom_pull(task_ids='fetch_data')
    conn = psycopg2.connect(
        dsn="postgresql://Moexdb_owner:Z6x5RkhAGJQm@ep-old-sound-a2zto35h.eu-central-1.aws.neon.tech/Moexdb?sslmode=require"
    )
    cursor = conn.cursor()

    data_to_insert = [
        (price['ticker'], price['interval'], price['open'], price['close'], price['high'], price['low'], price['value'], price['volume'], price['begin'], price['ending'])
        for price in prices
        ]
    cursor.executemany(
        "INSERT INTO pract (ticker, interval, open, close, high, low, value, volume, begin, ending) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        data_to_insert
    )
    conn.commit()
    cursor.close()
    conn.close()

# Операторы Airflow
get_start_date = PythonOperator(
    task_id='get_start_date',
    python_callable=get_last_ending_date,
    dag=dag,
)

fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_moex_data,
    provide_context=True,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

get_start_date >> fetch_data >> load_data