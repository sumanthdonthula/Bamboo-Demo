import streamlit as st
from langchain.text_splitter import RecursiveCharacterTextSplitter
import PyPDF2
import io
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime
import pytz
import re
from dotenv import load_dotenv
import os
import json
from snowflake.snowpark.context import get_active_session


class DocumentDifferenceApp:
    def __init__(self, session, stage_path='pdf_store', database_name='BAMBOO', 
                 schema_name='BILLS', chunked_table='CHUNKED_PDF_SUM', 
                 summary_table='SUMMARIZED_CONTENT'):
        """
        Initializes the DocumentDifferenceApp with a Snowflake session and configuration parameters.
        """
        self.session = session
        self.stage_path = f"@{database_name}.{schema_name}.{stage_path}"
        self.database_name = database_name
        self.schema_name = schema_name
        self.chunked_table = chunked_table
        self.summary_table = summary_table

    def read_pdf(self, file_url):
        """
        Reads a PDF file from the given file URL and extracts text.
        """
        with self.session.file.get_stream(file_url) as file:
            buffer = io.BytesIO(file.read())

        reader = PyPDF2.PdfReader(buffer)
        text = ""
        for page in reader.pages:
            try:
                text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
            except:
                text = "Unable to Extract"

        return text

    def process_load(self, file_url):
        """
        Processes and loads the PDF text into the database if not already present.
        """
        file_selected = file_url.replace(f'{self.stage_path}/', '').replace('.pdf', '')

        file_list_summarized = self.session.sql(f"""
            SELECT DISTINCT "file_name" 
            FROM {self.database_name}.{self.schema_name}.{self.chunked_table}
        """).to_pandas()['file_name'].tolist()

        if file_selected not in file_list_summarized:
            text = self.read_pdf(file_url)
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=30000, 
                chunk_overlap=1000, 
                length_function=len
            )
            chunks = text_splitter.split_text(text)
            df = pd.DataFrame(chunks, columns=['chunks'])
            df['file_path'] = file_url
            df['file_name'] = file_selected
            df['date'] = datetime.now(pytz.timezone("America/Chicago")).strftime("%Y-%m-%d")
            df['time'] = datetime.now(pytz.timezone("America/Chicago")).strftime("%I:%M:%S %p")

            tbl_write = self.session.create_dataframe(df)
            tbl_write.write.mode("append").save_as_table(f"{self.database_name}.{self.schema_name}.{self.chunked_table}")

    def split_text(self, text):
        """
        Splits the text to get the PDF name.
        """
        return text.split("pdf_store/")[1]

    def summarize(self, file_name):
        """
        Summarizes the chunks of the specified PDF file.
        """
        file_selected = file_name.replace('.pdf', '')
        file_list_summarized = self.session.sql(f"""
            SELECT DISTINCT "file_name" 
            FROM {self.database_name}.{self.schema_name}.{self.summary_table}
        """).to_pandas()['file_name'].tolist()

        if file_selected in file_list_summarized:
            summary_df = self.session.sql(f"""
                SELECT "file_name", LISTAGG(summarized_chunk, '|') as summary 
                FROM {self.database_name}.{self.schema_name}.{self.summary_table} 
                WHERE "file_name" = '{file_selected}' 
                GROUP BY "file_name"
            """).to_pandas()
        else:
            insert_sql = self.session.sql(f"""
                SELECT "file_name", SNOWFLAKE.CORTEX.SUMMARIZE("chunks") as summarized_chunk, "chunks" 
                FROM {self.database_name}.{self.schema_name}.{self.chunked_table} 
                WHERE "file_name" = '{file_selected}'
            """).to_pandas()
            tbl_write = self.session.create_dataframe(insert_sql)
            tbl_write.write.mode("append").save_as_table(f"{self.database_name}.{self.schema_name}.{self.summary_table}")

            summary_df = self.session.sql(f"""
                SELECT "file_name", LISTAGG(summarized_chunk, '|') as summary 
                FROM {self.database_name}.{self.schema_name}.{self.summary_table} 
                WHERE "file_name" = '{file_selected}' 
                GROUP BY "file_name"
            """).to_pandas()

        return summary_df

    @staticmethod
    def format_paragraphs(input_string, delimiter):
        """
        Formats the input string into paragraphs separated by the delimiter.
        """
        parts = input_string.split(delimiter)
        formatted_string = "\n\n".join(part.strip() for part in parts)
        return formatted_string

    def get_answer_reka(self, summary1, summary2, option1, option2):
        """
        Compares two summaries and returns the differences using REKA.
        """
        summary1 = re.sub(r'[^a-zA-Z0-9\s]', '', summary1)
        summary2 = re.sub(r'[^a-zA-Z0-9\s]', '', summary2)

        if summary1 == summary2:
            return "Both the Documents are Same"
        else:
            instructions = f"""
            You are given 2 Documents in {option1} and {option2} respectively.        
            {option1}: <Question> {summary1} </Question>.
            {option2}: <context> {summary2} </context>.

            Find and Highlight the key differences in both the Documents.
            """

            response_df = self.session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('reka-flash', '{instructions}') as response
            """).to_pandas()

            return response_df['RESPONSE'][0]

    def get_doc_list(self):
        """
        Retrieves the list of document names from the storage.
        """
        doc_names = self.session.sql(f"list {self.stage_path}").collect()
        doc_df = pd.DataFrame(doc_names)
        doc_list = doc_df['name'].apply(self.split_text)
        return doc_list

def main():
    """
    Main function to run the Streamlit app.
    """
    # Define the path to the .env file
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')

    # Load the .env file
    load_dotenv(dotenv_path=env_path)

    

    try:

        # Define the path to the .env file
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')

        if os.path.exists(env_path):
            # Load the .env file
            load_dotenv(dotenv_path=env_path)
        
            connection_parameters = {
        "user": os.getenv("user"),
        "password": os.getenv("password"),
        "account": os.getenv("account"),
        "role": os.getenv("role"),
        "warehouse": os.getenv("warehouse"),
        "database": os.getenv("database"),
        "schema": os.getenv("schema"),
    }
            session = Session.builder.configs(connection_parameters).create()
        else:

            connection_parameters = {
        "user": st.secrets.db_credentials.SNOWFLAKE_USER,
        "password": st.secrets.db_credentials.SNOWFLAKE_PASSWORD,
        "account": st.secrets.db_credentials.SNOWFLAKE_ACCOUNT,
        "role": st.secrets.db_credentials.SNOWFLAKE_ROLE,
        "warehouse": st.secrets.db_credentials.SNOWFLAKE_WAREHOUSE,
        "database": st.secrets.db_credentials.SNOWFLAKE_DATABASE,
        "schema":st.secrets.db_credentials.SNOWFLAKE_SCHEMA,
    }
            
            session = Session.builder.configs(connection_parameters).create()
    except:
        session = get_active_session()

    with open('config_file.json', 'r') as f:
        config = json.load(f)

    compare_app_config = config['compare_app']
    db_schema=config['db_schema']
    stage_path = compare_app_config['stage_path']
    database_name = db_schema['database_name']
    schema_name = db_schema['schema_name']
    chunked_table = compare_app_config['chunked_table']
    summary_table = compare_app_config['summary_table']

    compare_app = DocumentDifferenceApp(
        session=session,
        stage_path=stage_path,
        database_name=database_name,
        schema_name=schema_name,
        chunked_table=chunked_table,
        summary_table=summary_table
    )

    doc_list = compare_app.get_doc_list()

    st.write("# Greetings👋")
    intro_message = """
    Want to compare the key changes in corresponding Documents?📄.

    Select two documents and I'll do my best to contrast the changes.
    """
    st.write(intro_message)
    st.write("---------")

    option1 = st.selectbox('Select Document 1 to Compare', doc_list, index=None)
    option2 = st.selectbox('Select Document 2 to Compare', doc_list, index=None)

    if option1 and option2:
        file_path_1 = f"{compare_app.stage_path}/{option1}"
        file_path_2 = f"{compare_app.stage_path}/{option2}"

        compare_app.process_load(file_path_1)
        compare_app.process_load(file_path_2)

        st.write('Doc1 selected 📝: ', option1)
        st.write('Doc2 selected 📝: ', option2)

        summary_df_1 = compare_app.summarize(option1)
        summary_df_2 = compare_app.summarize(option2)

        formatted_summary_1 = compare_app.format_paragraphs(summary_df_1['SUMMARY'][0], '|')
        formatted_summary_2 = compare_app.format_paragraphs(summary_df_2['SUMMARY'][0], '|')

        response = compare_app.get_answer_reka(formatted_summary_1, formatted_summary_2, option1, option2)
        st.write(response)

if __name__ == "__main__":
    main()
