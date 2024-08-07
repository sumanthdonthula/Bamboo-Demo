import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from langchain.text_splitter import RecursiveCharacterTextSplitter
import PyPDF2
import io
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
import json


class RAGSearchApp:
    def __init__(self, session, slide_window_hist=3, model_name='llama3.1-70b', 
                 stage_path='pdf_store', database_name='BAMBOO', schema_name='BILLS', 
                 chunk_table_name='CHUNKED_PDF_RAG', vector_store_table='VECTOR_STORE_RAG', 
                 embed_model_name='e5-base-v2'):
        """
        Initializes the RAGSearchApp with the session, sliding window history, and model name.
        """
        self.session = session
        self.slide_window_hist = slide_window_hist
        self.model_name = model_name
        self.stage_path_url = f"@{database_name}.{schema_name}.{stage_path}"
        self.stage_path=stage_path
        self.database_name = database_name
        self.schema_name = schema_name
        self.chunk_table_name = chunk_table_name
        self.vector_store_table = vector_store_table
        self.embed_model_name = embed_model_name

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

    def split_text(self,text):
        """
        Splits text to get the PDF name.
        """
        return text.split(f"{self.stage_path}/")[1]

    @staticmethod
    def append_text(text):
        """
        Appends .pdf to the text.
        """
        return text + ".pdf"

    def load_pdf_and_vectorize(self):
        """
        Loads PDFs and vectorizes them for search.
        """

        doc_names = self.session.sql(f"list {self.stage_path_url}").collect()
        doc_df = pd.DataFrame(doc_names)

        doc_list = doc_df['name'].apply(self.split_text).to_list()

        doc_list_chunked = self.session.sql(
            f'SELECT DISTINCT "file_name" from "{self.database_name}"."{self.schema_name}"."{self.chunk_table_name}"').to_pandas()
        doc_list_chunked['file_name'] = doc_list_chunked['file_name'].apply(self.append_text)
        doc_list_chunked = doc_list_chunked['file_name'].to_list()
        dif_list = [x for x in doc_list if x not in doc_list_chunked]

        for file_name in dif_list:
            file_url = f'{self.stage_path_url}/{file_name}'
            text = self.read_pdf(file_url)

            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=10000,  # Adjust this as you see fit
                chunk_overlap=500,  # This lets text have some form of overlap. Useful for keeping chunks contextual
                length_function=len
            )

            chunks = text_splitter.split_text(text)
            df = pd.DataFrame(chunks, columns=['chunks'])
            df['file_path'] = file_url
            df['file_name'] = file_url.split("/")[1].replace('.pdf', '')

            chicago_timezone = pytz.timezone("America/Chicago")
            # Get the current time in Chicago
            chicago_time = datetime.now(chicago_timezone)
            df['date'] = chicago_time.strftime("%Y-%m-%d")
            df['time'] = chicago_time.strftime("%I:%M:%S %p")

            tbl_write = self.session.create_dataframe(df)
            tbl_write.write.mode("append").save_as_table(f"{self.database_name}.{self.schema_name}.{self.chunk_table_name}")

            file_name_without_pdf = file_name.replace('.pdf', '')

            temp_sql = self.session.sql(
                f'''select "file_name","chunks",SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.embed_model_name}', "chunks") as vector_embedings 
                from {self.database_name}.{self.schema_name}.{self.chunk_table_name} where "file_name"='{file_name_without_pdf}' ''').to_pandas()
            tbl_write = self.session.create_dataframe(temp_sql)
            tbl_write.write.mode("append").save_as_table(f"{self.database_name}.{self.schema_name}.{self.vector_store_table}")

    def summarize_question_with_history(self, chat_history, question):
        """
        Summarizes the question with the chat history to provide context.
        """
        prompt_template = """
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
        prompt = prompt_template.format(chat_history=chat_history, question=question)

        cmd = "select snowflake.cortex.complete(?, ?) as response"
        df_response = self.session.sql(cmd, params=[self.model_name, prompt]).collect()
        summary = df_response[0].RESPONSE

        return summary.replace("'", "")

    def get_similar_chunks(self, question):
        """
        Retrieves similar chunks from the vector store based on the question.
        """
        cmd = f"""
        with results as
        (SELECT "file_name",
           VECTOR_COSINE_SIMILARITY("VECTOR_EMBEDINGS"::VECTOR(FLOAT, 768),
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.embed_model_name}', ?)) as similarity,
           "chunks"
        from {self.database_name}.{self.schema_name}.{self.vector_store_table}
        order by similarity desc
        limit 1)
        select "chunks", "file_name" from results 
        """
        df_chunks = self.session.sql(cmd, params=[question]).to_pandas()

        context = df_chunks['chunks'][0].replace("'", "")
        file_name = df_chunks['file_name'][0]

        return context, file_name

    def complete(self, myquestion):
        """
        Completes the query using the language model and returns the response and file name.
        """
        prompt, file_name = self.create_prompt(myquestion)
        cmd = "select snowflake.cortex.complete(?, ?) as response"
        list_response = self.session.sql(cmd, params=[self.model_name, prompt]).collect()
        return list_response[0]['RESPONSE'], file_name

    def create_prompt(self, myquestion):
        """
        Creates the prompt for the language model based on the question and chat history.
        """
        chat_history = ""
        if st.session_state.use_chat_history:
            st_session = StreamlitSession(self.slide_window_hist)
            chat_history = st_session.get_chat_history()
            if chat_history:
                question_summary = self.summarize_question_with_history(chat_history, myquestion)
                prompt_context, file_name = self.get_similar_chunks(question_summary)
            else:
                prompt_context, file_name = self.get_similar_chunks(myquestion)
        else:
            prompt_context, file_name = self.get_similar_chunks(myquestion)

        prompt_template = """
        You are an expert chat assistant that extracts information from the CONTEXT provided
        between <context> and </context> tags.
        You offer a chat experience considering the information included in the CHAT HISTORY
        provided between <chat_history> and </chat_history> tags.
        When answering the question contained between <question> and </question> tags
        be concise and do not hallucinate. 
        If you don’t have the information just say so.

        Do not answer any question outside the CONTEXT.
        Do not mention the CONTEXT used in your answer.
        Do not mention the CHAT HISTORY used in your answer.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <context>
        {prompt_context}
        </context>
        <question>
        {myquestion}
        </question>
        Answer:
        """
        prompt = prompt_template.format(chat_history=chat_history, prompt_context=prompt_context, myquestion=myquestion)
        return prompt, file_name


class StreamlitSession:
    def __init__(self, slide_window):
        """
        Initializes the Streamlit session with the sliding window for chat history.
        """
        self.slide_window = slide_window

    def init_session_state(self):
        """
        Initializes session state variables.
        """
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "use_chat_history" not in st.session_state:
            st.session_state.use_chat_history = True

    def init_messages(self):
        """
        Initializes messages in session state.
        """
        if "clear_conversation" in st.session_state and st.session_state.clear_conversation:
            st.session_state.messages = []

    def get_chat_history(self):
        """
        Retrieves the chat history from the session state.
        """
        chat_history = []
        start_index = max(0, len(st.session_state.messages) - self.slide_window)

        for i in range(start_index, len(st.session_state.messages)):
            chat_history.append(st.session_state.messages[i]["content"])

        return chat_history


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

    rag_app_config = config['rag_app']
    db_schema=config['db_schema']
    slide_window_hist = rag_app_config['slide_window_hist']
    model_name = rag_app_config['model_name']
    stage_path = rag_app_config['stage_path']
    database_name = db_schema['database_name']
    schema_name = db_schema['schema_name']
    chunk_table_name = rag_app_config['chunk_table_name']
    vector_store_table = rag_app_config['vector_store_table']
    embed_model_name = rag_app_config['embed_model_name']

    rag_object = RAGSearchApp(
        session=session,
        slide_window_hist=slide_window_hist,
        model_name=model_name,
        stage_path=stage_path,
        database_name=database_name,
        schema_name=schema_name,
        chunk_table_name=chunk_table_name,
        vector_store_table=vector_store_table,
        embed_model_name=embed_model_name
    )

    rag_object.load_pdf_and_vectorize()

    st_session = StreamlitSession(rag_object.slide_window_hist)
    st_session.init_session_state()

    st.title("💬 Gen AI Assistant")
    st.markdown("<h3 style='font-size:14px;'>Welcome to the Gen AI Assistant! Please ask me questions about the bills, laws, or docs you might be interested today!</h3>", unsafe_allow_html=True)

    if session:
        st_session.init_messages()

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if question := st.chat_input("What do you want to know about today"):
            st.session_state.messages.append({"role": "user", "content": question})
            with st.chat_message("user"):
                st.markdown(question)
            with st.chat_message("assistant"):
                question = question.replace("'", "")
                with st.spinner("Bamboo AI thinking..."):
                    response, file_name = rag_object.complete(question)
                    res_text = response.replace("'", "")
                    st.write("Reference Document: ", file_name)
                    st.write(res_text)

            st.session_state.messages.append({"role": "assistant", "content": f"Reference Doc: {file_name}\n{res_text}"})
        session.close()
    else:
        st.error("Failed to connect to Snowflake.")


if __name__ == "__main__":
    main()
