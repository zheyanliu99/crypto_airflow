from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

# connect to aws postgres
import psycopg2
import pandas as pd

dsn = "postgresql://zl3119:1947@w4111.cisxo09blonu.us-east-1.rds.amazonaws.com/proj1part2"
conn = psycopg2.connect(dsn)


import datetime
import csv
import requests
import json

# dd/mm/YY
date = (datetime.date.today()- datetime.timedelta(days = 1)).strftime("%Y-%m-%d")


# function to insert pandas df to database
import psycopg2.extras as extras
from io import StringIO

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close() 

# get emails in config/variables_config.json
try:
    dag_config = Variable.get('variables_config', deserialize_json=True)
    email_list = dag_config['email_list']
except:
    email_list = ['yan.test@gmail.com', 'zheyan.test@gmail.com', 'zheyan.test@yahoo.com']
    raise Warning("You didn't set up variables_fig, using default email_list ['yan.test@gmail.com', 'zheyan.test@gmail.com', 'zheyan.test@yahoo.com']")

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1)
}

def get_crypto():
    # date = '2022-04-28'
    url = 'https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{}?adjusted=true&apiKey=2gEOah0DjDSI9ImMCR_0BL5TSJkvlC4z'.format(date)
    r = requests.get(url)
    data = r.json()
    
    if data['status'] == 'ERROR':
        raise(data['error'])
    # %% transform data
    data['results']
    exchange_symbol = []
    close_price = []
    highest_price = []
    lowest_price = []
    transactions = []
    weighted_average = []

    for cr in data['results']:
        # print(cr)
        exchange_symbol.append(cr['T'])
        close_price.append(cr['c'])
        highest_price.append(cr['h'])
        lowest_price.append(cr['l'])
        transactions.append(cr['n'])
        if 'vw' in cr:
            weighted_average.append(cr['vw'])
        else:
            weighted_average.append(-999)

    cr_dict = {
        'exchange_symbol':exchange_symbol,
        'close_price':close_price,
        'highest_price':highest_price,
        'lowest_price':lowest_price,
        'transactions':transactions,
        'weighted_average':weighted_average}

    cr_df = pd.DataFrame(cr_dict)
    cr_df['date'] = date

    # Insert values to df
    execute_values(conn, cr_df, 'crytpo_daily')
        

def check_email(**context):
    
    # email_list = ['yan.test@gmail.com', 'zheyan.test@gmail.com']
    cur = conn.cursor()
    cur.execute("SELECT * FROM email_sent WHERE date = %s", (date, ))
    sent_emails = cur.fetchall()
    sent_emails_list = [email[1] for email in sent_emails]

    sending_emails = [email for email in email_list if email not in sent_emails_list]
    if not sending_emails:
        raise ValueError('All the listed emails recieved crypto info today')
    return ','.join(sending_emails)


def update_sent_emails(**context):
    with open(r'/opt/airflow/dags/files/email_sent.csv', 'a') as file:
        writer = csv.writer(file)
        year, week = context['ti'].xcom_pull(task_ids='downloading_books')
        for email in context['ti'].xcom_pull(task_ids='check_emails').split(','):
            if email:
                newrow = [email, year, week]
                writer.writerow(newrow)


with DAG("crpto_pipline", start_date=datetime.datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    get_crypto = PythonOperator(
        task_id="get_crypto",
        python_callable=get_crypto
    )

    
    check_emails = PythonOperator(
        task_id="check_emails",
        python_callable=check_email
    )

    # send_emails = EmailOperator(
    #     task_id='send_emails',
    #     to="{{ task_instance.xcom_pull(task_ids='check_emails') }}",
    #     subject="New York Times Weekly Bestseller Books",
    #     files = ['/opt/airflow/dags/files/combined-print-and-e-book-nonfiction.csv', '/opt/airflow/dags/files/combined-print-and-e-book-fiction.csv'],
    #     html_content="<h3>Check out attachments for the bestseller books this week!</h3>"
    # )

    # update_sent_emails = PythonOperator(
    #     task_id="update_sent_emails",
    #     python_callable=update_sent_emails
    # )
    
    get_crypto > check_emails