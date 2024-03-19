from utils import *

from openai import OpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.utilities import SQLDatabase
from langchain import OpenAI
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI

import streamlit as st
import os

def load_data():
    root = os.getcwd()
    # Defining service account file path
    service_account_file = os.path.join(root, "credentials_2.json")

    # Connect to BigQuery
    bq_client = connect_to_bigquery(service_account_file)

    # Define BigQuery dataset details - project, dataset, and table
    project = "data-23-24"
    dataset = "invoice_dataset"
    table = "bu18dec"
    sqlite_row_count = get_row_count()
    # print(row_count, type(row_count))

    # Define BigQuery query
    query = f"SELECT * FROM `{project}.{dataset}.{table}` ORDER BY PNR DESC LIMIT 5000 OFFSET {sqlite_row_count+1};"
    # query = f"SELECT * FROM `{project}.{dataset}.{table}` ORDER BY PNR DESC;"

    # Execute BigQuery query and fetch data
    df = execute_bigquery_query(bq_client, query)

    # Clean the data
    # df = df.fillna('', inplace=True)
    df_cleaned = clean_data(df)

    # Save the cleaned data to SQLite database
    db_file = 'invoicedb.sqlite'
    save_to_sqlite(df_cleaned, db_file)

    # print("SQLite row count = ", sqlite_row_count)
    # print("BIGQ row count = ", bigq_row_count)


# from apscheduler.schedulers.blocking import BlockingScheduler

# scheduler = BlockingScheduler()
# scheduler.add_job(load_data, 'interval', minutes=1)
# scheduler.start()
# scheduler.end()





