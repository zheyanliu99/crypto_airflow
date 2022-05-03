# import airflow packages
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

# packages for connection to aws postgres
import psycopg2
import psycopg2.extras as extras


# other packages
import pandas as pd
import datetime
import requests

dsn = "postgresql://zl3119:1947@w4111.cisxo09blonu.us-east-1.rds.amazonaws.com/proj1part2"
conn = psycopg2.connect(dsn)
cur = conn.cursor()

# yesterday's date YYYY-mm-dd
date = (datetime.date.today()- datetime.timedelta(days = 1)).strftime("%Y-%m-%d")


# function to insert pandas df to database
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

# get emails list in config/variables_config.json. 
# Error in this step means that you have not set up variables as in tutorial
try:
    dag_config = Variable.get('variables_config', deserialize_json=True)
    email_list = dag_config['email_list']
    print(email_list)
except:
    email_list = ['yan.test@gmail.com', 'zheyan.test@gmail.com', 'zheyan.test@yahoo.com']
    raise Warning("You didn't set up variables_fig, using default email_list ['yan.test@gmail.com', 'zheyan.test@gmail.com', 'zheyan.test@yahoo.com']")


# Task 1: Get crypto
def get_crypto():
    
    url = 'https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{}?adjusted=true&apiKey=2gEOah0DjDSI9ImMCR_0BL5TSJkvlC4z'.format(date)
    r = requests.get(url)
    data = r.json()
    
    # the api has limit 5 times/minuate
    if data['status'] == 'ERROR':
        raise(data['error'])

    # transform data
    data['results']
    exchange_symbol = []
    close_price = []
    highest_price = []
    lowest_price = []
    transactions = []
    weighted_average = []

    for cr in data['results']:
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

    # Insert values to table crytpo_daily, ignore this step if violate primary key constraint
    try:
        execute_values(conn, cr_df, 'crytpo_daily')
    except:
        print('Primary key exists')
    cr_df.to_csv(f'/opt/airflow/dags/files/crypto_{date}.csv', index = False)
        
# Task2: give emails that not recieved any info for the given date
def check_email(**context):
    cur = conn.cursor()
    cur.execute("SELECT * FROM email_sent WHERE date = %s", (date, ))
    sent_emails = cur.fetchall()
    sent_emails_list = [email[1] for email in sent_emails]

    sending_emails = [email for email in email_list if email not in sent_emails_list]
    # if all email sent, raise error to stop the DAG
    if not sending_emails:
        raise ValueError('All the listed emails recieved crypto info today')
    # return a str of emails, you can use xcom to get this variable
    return ','.join(sending_emails)

# Task4: if email sent successful to some users, update that in the database, so they will not recieve email in the same date
def update_sent_emails(**context):
    for email in context['ti'].xcom_pull(task_ids='check_emails').split(','):
        cur.execute("INSERT INTO email_sent values(%s, %s)", (date, email))
    conn.commit()

# Create a DAG

# default args for the DAG
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1)
}


with DAG("crypto_pipeline", start_date=datetime.datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    get_crypto = PythonOperator(
        task_id="get_crypto",
        python_callable=get_crypto
    )

    
    check_emails = PythonOperator(
        task_id="check_emails",
        python_callable=check_email
    )

    # Task3: send email to selected users
    send_emails = EmailOperator(
        task_id='send_emails',
        to="{{ task_instance.xcom_pull(task_ids='check_emails') }}",
        subject="Crypto price for Yesterday",
        files = [f'/opt/airflow/dags/files/crypto_{date}.csv'],
        html_content="<h3>Check out Crypto price!</h3>"
    )

    update_sent_emails = PythonOperator(
        task_id="update_sent_emails",
        python_callable=update_sent_emails
    )
    
    get_crypto >> check_emails >> send_emails >> update_sent_emails