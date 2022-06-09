from datetime import date
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import credentials as creds
import sql_queries as sql

"""
RUN CMD commands to initialize MYSQL DB:
    docker pull mysql
    docker run --name mysql_db -p 3366:3306 -e MYSQL_ROOT_PASSWORD=password -d mysql
"""

upload_date = date.today()
upload_date_str = str(upload_date)


def get_db_connection():
    cnx = pymysql.connect(host=creds.credentials['host'], port=creds.credentials['port'], user=creds.credentials['user'],
                          passwd=creds.credentials['password'], db=creds.credentials['database'])
    cur = cnx.cursor()
    return cnx, cur


def write_df_to_table(df, table):
    try:
        url = 'mysql+pymysql://{}:{}@{}:{}/{}'
        url = url.format(creds.credentials['user'], creds.credentials['password'], creds.credentials['host'],
                         creds.credentials['port'], creds.credentials['database'])
        engine = create_engine(url, echo=False)
        df.to_sql(name=table, con=engine, if_exists='replace', index=False, chunksize=100000, method='multi')
    except ValueError as e:
        return f'{e}'
    return f"Data written to database table {table}"


def run_query(query):
    try:
        cnx, cur = get_db_connection()
        result = cur.execute(query)
        close_con = True
    except pymysql.DatabaseError as e:
        print(f'Connection error: {e}')

    if close_con:
        cnx.close()

    return result


def prepare_output_data_for_load_to_db():
    try:
        df = pd.read_csv('output.csv')
    except FileNotFoundError as e:
        return f"{e}"
    try:
        df['datetime'] = pd.to_datetime(df.Date.astype(str) + ' ' + df.hour.astype(str))
        df['audit_loaded_datetime'] = pd.to_datetime(upload_date_str)
        df = df[['datetime', 'impression_count', 'click_count', 'audit_loaded_datetime']]
    except ValueError as e:
        return f"Data transformation failed: {e}"
    return df


def test_get_db_load_result(query, df):
    db_table_rows = run_query(query)
    df_rows = len(df)
    if db_table_rows == df_rows:
        return "Job Suceeded"
    else:
        return "Job Failed"


def load_data_to_mysql(table):
    run_query(sql.query_1)
    df = prepare_output_data_for_load_to_db()
    write_df_to_table(df, table)
    load_result = test_get_db_load_result(sql.query_2, df)
    return load_result


print(load_data_to_mysql('activities'))


