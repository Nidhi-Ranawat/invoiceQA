from google.cloud import bigquery
from google.oauth2 import service_account
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlite3

from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.utilities import SQLDatabase
from langchain import OpenAI
from langchain.prompts import PromptTemplate
import streamlit as st

def connect_to_bigquery(credentials_file):
    # Getting credentials from service account file
    credentials = service_account.Credentials.from_service_account_file(credentials_file)
    # Creating BigQuery client with credentials (connection)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

def execute_bigquery_query(client, query):
    # Executing the query and converting the result to a DataFrame
    return client.query(query).to_dataframe()

@st.cache_data
def clean_data(df):
    # Cleaning PNR numbers for unexpected values
    # df['PNR'] = df['PNR'].apply(clean_invalid_chars)

    # DATE FORMATTING
    date_columns = ['Date', 'Start Date', 'End Date', 'Paid Date', 'Received Date']
    df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce', format="%Y-%m-%d")

    # FLOAT VALUES
    float_columns = ['Base Fare', 'Airport and Other Taxes', 'Service Fee', 'Cancellation Charge', 
                    'GST Out', 'Total Inv', 'Rate Per Adlt', 'Rate Per Child', 'Rate Per Infant', 
                    'Tax Per Adult', 'Tax Per Child', 'Tax Per Infant', 'Cost in FC', 'Exchange Rate', 
                    'Amt In INR', 'Amt Received']
    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors='coerce')

    # Converting Int64 columns to integers
    int_columns = ['Credit Days', 'Real Credit', 'Segments', 'Service Fee Rev']
    df[int_columns] = df[int_columns].fillna(0).astype(int)

    # Converting object columns to string
    object_columns = df.select_dtypes(include=['object']).columns
    df[object_columns] = df[object_columns].fillna('').astype(str)
    
    return df

@st.cache_data
def save_to_sqlite(df, db_file):
    # Creating a SQLite database connection
    conn = sqlite3.connect(db_file)
    # Save DataFrame to SQLite database
    # df.to_sql('invoices', conn, if_exists='append', index=False)
    df.to_sql('invoices', conn, if_exists='replace', index=False)
    conn.close()

def get_row_count():
    conn = sqlite3.connect('invoicedb.sqlite')
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(*) FROM invoices')
    except:
        return 0
    count = cursor.fetchone()[0]
    conn.close()
    return count

def extract_info(query):
    print("extracting")
    conn = sqlite3.connect('invoicedb.sqlite')

    headers = pd.read_sql_query('PRAGMA table_info(invoices);', conn, parse_dates=["E"])
    columns = headers["name"]
    cols = columns.tolist()

    # x = pd.read_sql_query('SELECT Customer, `Passenger Name`, PNR, `Ticket No`, `Base Fare`, `Total Inv`, Agent, `Amt In INR` FROM invoices LIMIT 1000', conn, parse_dates=["E"])

    template ="""
    Compose a more suitable SQLite query for {query} with consideration for column references from {cols} and use wildcards if where condition.
    Ensure the query is case insensitive.
    The query should be designed for the 'invoices' table and utilize backticks when selecting columns in the final query.
    """

    # prompt_template = PromptTemplate(input_variables=["query","x"], template=template)
    prompt_template = PromptTemplate(input_variables=["query","cols"], template=template)

    llm = OpenAI(temperature=0,max_tokens=1000)
    ans = llm(prompt_template.format(query=query,cols=cols))
    print("=================================================================")
    print(ans)

    df = pd.read_sql_query(ans, conn, parse_dates=["E"])
    print(df)
    return df
