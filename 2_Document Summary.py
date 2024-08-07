import streamlit as st
from snowflake.snowpark import Session
from langchain.text_splitter import RecursiveCharacterTextSplitter
import PyPDF2
import io
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
import json
from snowflake.snowpark.context import get_active_session

class SummaryApp:
    def __init__(self, session, stage_path='@pdf_store', database_name='BAMBOO', 
                 schema_name='BILLS', chunked_table='CHUNKED_PDF_SUM', 
                 summary_table='SUMMARIZED_CONTENT'):
        """
        Initializes the SummaryApp with a Snowflake session and configuration parameters.
        """
        self.session = session
        self.stage_path_url = f"@{database_name}.{schema_name}.{stage_path}"
        self.stage_path=stage_path
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
        return text.split(f"{self.stage_path}/")[1]

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

    def get_doc_list(self):
        """
        Retrieves the list of document names from the storage.
        """
        doc_names = self.session.sql(f"list {self.stage_path_url}").collect()
        doc_df = pd.DataFrame(doc_names)
        doc_list = doc_df['name'].apply(self.split_text)
        return doc_list


def main():
    """
    Main function to run the Streamlit app.
    """
    

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

    summary_app_config = config['summary_app']
    db_schema=config['db_schema']
    stage_path = summary_app_config['stage_path']
    database_name = db_schema['database_name']
    schema_name = db_schema['schema_name']
    chunked_table = summary_app_config['chunked_table']
    summary_table = summary_app_config['summary_table']

    summary_app = SummaryApp(
        session=session,
        stage_path=stage_path,
        database_name=database_name,
        schema_name=schema_name,
        chunked_table=chunked_table,
        summary_table=summary_table
    )

    doc_list = summary_app.get_doc_list()

    st.write("# Hey there👋")

    intro_message = """
    Have you ever had trouble reading a document 📄 and understanding it?

    Let me know, I can read documents for you and summarize them!
    """
    st.write(intro_message)
    st.write("---------")

    option = st.selectbox('What Document Would you like to Summarize?', doc_list, index=None)

    if option:
        file_path = f"@pdf_store/{option}"
        summary_app.process_load(file_path)
        st.write('You selected 📝:', option)
        summary_df = summary_app.summarize(option)
        formatted_summary = summary_app.format_paragraphs(summary_df['SUMMARY'][0], '|')
        st.write(formatted_summary)


if __name__ == "__main__":
    main()
