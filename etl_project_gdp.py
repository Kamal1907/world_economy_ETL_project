# Importing required libraries
import requests
from datetime import datetime
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3

# defining necessary variables
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

table_attribs = ['Country', 'GDP_USD_millions']

db_name = 'World_Economies.db'

table_name = 'Countries_by_GDP'

csv_path = '/Users/Support/Desktop/Data Engineering/Countries_by_GDP.csv'

# defining a function to fecth table from the web 
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country" : col[0].a.contents[0],
                            "GDP_USD_millions" : col[2].contents[0]}
                df_dd = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df_dd], ignore_index = True)
    return df

# defining a function to transformation GDP column in the table
def transform(df):
    gdp_list = df['GDP_USD_millions'].to_list()
    gdp_list = [float("".join(x.split(','))) for x in gdp_list]
    gdp_list = [np.round(x/1000,2) for x in gdp_list]
    df['GDP_USD_millions'] = gdp_list
    df = df.rename(columns = {'GDP_USD_millions' : 'GDP_USD_billions'})
    
    return df

# defining a function to load the table into csv
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

# defining a function to load the table to database
def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)

# defining a function to run sql query
def run_query(query_stm, sql_conn):
    query_output = pd.read_sql(query_stm, sql_conn)
    print(f"SQL statement:\n{query_stm}\n")
    print(f"Query output:\n{query_output}")

# defining a function to log all processes
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('./etl_project_log.txt', 'a') as file:
        file.write(timestamp + ':' + message + '\n')

# Running Processes
        
log_progress("Processes initiated")

df = extract(url, table_attribs)

log_progress("Preliminaries complete.\n Initiating ETL process...")

df = transform(df)

log_progress("Data transformation complete.\n Initiating loading process...")

load_to_csv(df, csv_path)

log_progress("Data saved to CSV file.")

conn = sqlite3.connect(db_name)

log_progress("SQL Connection initiated.")

load_to_db(df, conn, table_name)

log_progress("Data loaded to Database as table. Running the query.")

query_stm = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"

run_query(query_stm, conn)

log_progress("Process Complete!")

conn.close()