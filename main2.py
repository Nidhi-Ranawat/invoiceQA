from utils import *

# from importBigQueryToSqlite import df_cleaned
from openai import OpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.utilities import SQLDatabase
from langchain import OpenAI
from langchain_community.agent_toolkits import create_sql_agent, SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain.agents.agent_types import AgentType
from dotenv import load_dotenv

import streamlit as st
import os
import re

import time
from typing import List
from langchain.callbacks.base import BaseCallbackHandler
from langchain.agents import create_sql_agent

# Set environment variable for OpenAI API key

load_dotenv()
openai_api_key = os.environ.get("OPENAI_API_KEY")

def main():
    db = SQLDatabase.from_uri('sqlite:///invoicedb.sqlite')

    st.set_page_config(page_title="Ask your Invoice Database")
    st.header("Ask your Invoice database 📈")

    FORMAT_INSTRUCTIONS = """
    Use the following format:
    Question: the input question you must answer
    Thought: you should always think about what to do
    Action: the action to take, [{tool_names}]
    Action Input: the input to the action
    Observation: the result of the action
    ... (this Thought/Action/Action Input/Observation can repeat N times)
    Thought: I now know the final answer
    Final Answer: Create an apt SQL query for a given human language query. Return the SQL query only for 25 records max include following columns only Customer, `Passenger Name`, PNR, `Ticket No`, `Base Fare`, `Total Inv`, Agent, `Amt In INR`
    it must meet the particular condition specified (if any)
    """

    llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.5)
    # agent_executor = create_sql_agent(llm, db=db, verbose=True,handle_parsing_errors=True)
    toolkit = SQLDatabaseToolkit(db=db,llm=llm)
    agent_executor = create_sql_agent(llm=llm,toolkit=toolkit,format_instructions=FORMAT_INSTRUCTIONS, verbose=True,agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION)

    user_question = st.text_input("Ask a question about your BigQuery: ")
    
    data = "Not a list"
    if user_question is not None and user_question != "":
        if "list" in user_question.lower():
            with st.spinner(text="In progress..."):
                query = agent_executor.run(
                    user_question
                    )
                data = extract_info(query)
                st.write(data)
        else:
            st.write(data)
                
    st.write(" ")
    st.write("For eg : show me one random PNR number")

if __name__ == "__main__":
    main()
