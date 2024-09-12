#!/usr/bin/env python
# coding: utf-8

# In[163]:


import pandas as pd
import requests 
import bs4
import numpy as np
from datetime import datetime 
import sqlite3


# In[164]:


csv_file  = "Countries_by_GDP.csv"
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
table_name = "Countries_by_GDP "
db_name = "World_Economies.db"
table_attribs = ["Country", "GDP_USD_millions"]
query_result = "etl_project_log.txt"


# In[165]:


def extract(url,table_attribs):
    df = pd.read_html(url)
    df = df[3].iloc[:, [0,2]]
    df.columns = table_attribs
    return df


# In[170]:


def transform(df):
    column_to_clean = df.columns[1]
    df[column_to_clean]  = pd.to_numeric(df[column_to_clean], errors="coerce")
    # after clean
    df_cleaned = df.dropna(subset=[column_to_clean])
    df_cleaned[column_to_clean] = (df_cleaned[column_to_clean]/1000).round(2)
    df_cleaned = df_cleaned.rename(columns={column_to_clean :"GDP_USD_Billions"})
    return df_cleaned


# In[172]:


def load_to_csv(df, csv_path):
    df.to_csv(csv_path,index=False)


# In[173]:


def load_to_database(df, sql_connection,table):
    df.to_sql(table, sql_connection, if_exists="replace",index=False)
    


# In[174]:


def run_query(query_statement, sql_connection):
    query_output  = pd.read_sql(query_statement,sql_connection)
    print(query_output)


# In[1]:


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


# In[ ]:


df = extract(url, table_attribs)
log_progress('Preliminaries complete. Initiating ETL process')


# In[ ]:


clean_df = transform(df)
log_progress('Data extraction complete. Initiating Transformation process')


# In[175]:


load_to_csv(clean_df, csv_file)
log_progress('Data saved to CSV file')


# In[176]:


conn  = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')


# In[177]:


load_to_database(clean_df,conn, table_name)
log_progress('Data loaded to Database as table. Running the query')


# In[195]:


query_statement = f"select * from \"{table_name}\" where  \"GDP_USD_Billions\" > 5000 "


# In[196]:


query_statement


# In[197]:


run_query(query_statement,conn)


# In[ ]:


log_progress('Process Complete.')

conn.close()
# In[ ]:




