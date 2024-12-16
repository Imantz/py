import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import numpy as np
import sqlite3

log_file = "log_file.txt"
target_file = "Largest_banks_data.csv" 
table_name = "Largest_banks"
database_name = "Banks.db"
wiki_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_csv = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

# Project tasks
# Task 1:
def log_progress(message):
    # format must be <time_stamp> : <message>
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 
    return

# Task 2:
def extract(url, table_attribs):
    df = pd.read_html(url)[1]
    df = df.rename(columns={"Bank name": "Name", "Market cap (US$ billion)": "MC_USD_Billion"})
    df = df[table_attribs]
    return df

# Task 3:
def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rate['GBP'], 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate['EUR'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate['INR'], 2)

    return df

# Task 4:
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

# Task 5:
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    return

# Task 6:
def run_query(query_statement, sql_connection):
    result = pd.read_sql(query_statement, sql_connection)
    print(result)
    return


log_progress("ETL Job Started")

log_progress("Extract phase Started") 
extracted_data = extract(wiki_url, ["Name","MC_USD_Billion"])
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data, exchange_csv)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_to_csv(transformed_data, target_file)
log_progress("Load phase Ended")

sql_connection = sqlite3.connect(database_name)

log_progress("Load to DB phase Started")
load_to_db(transformed_data, sql_connection, table_name)
log_progress("Load to DB phase Ended")

log_progress("Queries phase Started")
run_query("SELECT * FROM Largest_banks", sql_connection)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
run_query("SELECT Name from Largest_banks LIMIT 5", sql_connection)
log_progress("Queries phase Ended")

sql_connection.close()

log_progress("ETL Job Ended")

