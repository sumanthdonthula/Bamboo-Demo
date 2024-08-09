import json
from dotenv import load_dotenv
import os
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import snowflake.connector     

def main():

    connection_parameters = {
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "account": os.getenv("ACCOUNT"),
        "role": os.getenv("ROLE"),
        "warehouse":os.getenv("WAREHOUSE")
    }
    conn =  snowflake.connector.connect(user=connection_parameters["user"],
    password=connection_parameters["password"],
    account=connection_parameters["account"],
    warehouse=connection_parameters["warehouse"],
    role=connection_parameters["role"])
    
    with open('config_file.json', 'r') as f:
        config = json.load(f)

    db_schema=config['db_schema']
    db_name = db_schema['database_name']
    schema_name = db_schema['schema_name']
    
    summary_app_config = config['summary_app']
    stage_path_sum = summary_app_config['stage_path']
    chunked_pdf_sum = summary_app_config['chunked_table']
    summarized_content_sum = summary_app_config['summary_table']

    compare_app_config = config['compare_app']
    stage_path_dif = compare_app_config['stage_path']
    chunked_pdf_dif = compare_app_config['chunked_table']
    summarized_content_dif = compare_app_config['summary_table']

    rag_app_config = config['rag_app']
    table_chunked_pdf_rag = rag_app_config['chunk_table_name']
    table_vector_store_rag = rag_app_config['vector_store_table']
    stage_path_rag = rag_app_config['stage_path']

    sql_statements=[]

    database_smt=f"create database if not exists {db_name}"
    
    sql_statements.append(database_smt)


    schema_smt=f"create schema if not exists {db_name}.{schema_name}"

    sql_statements.append(schema_smt)

    create_stage_rag=f"""
    CREATE STAGE IF NOT EXISTS {db_name}.{schema_name}.{stage_path_rag}
    """
    sql_statements.append(create_stage_rag)
    create_stage_sum=f"""
    CREATE STAGE IF NOT EXISTS {db_name}.{schema_name}.{stage_path_sum}
    """
    sql_statements.append(create_stage_sum)
    create_stage_dif=f"""
    CREATE STAGE IF NOT EXISTS {db_name}.{schema_name}.{stage_path_dif}
    """
    sql_statements.append(create_stage_dif)
    create_table_chunked_pdf_rag = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{table_chunked_pdf_rag} (
    "chunks" VARCHAR(16777216),
    "file_path" VARCHAR(16777216),
    "file_name" VARCHAR(16777216),
    "date" VARCHAR(16777216),
    "time" VARCHAR(16777216)
);
"""
    sql_statements.append(create_table_chunked_pdf_rag)
    create_table_vector_store_rag = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{table_vector_store_rag} (
    "file_name" VARCHAR(16777216),
    "chunks" VARCHAR(16777216),
    VECTOR_EMBEDINGS VARIANT
);
"""
    sql_statements.append(create_table_vector_store_rag)
    create_table_chunked_pdf_dif = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{chunked_pdf_dif} (
    "chunks" VARCHAR(16777216),
    "file_path" VARCHAR(16777216),
    "file_name" VARCHAR(16777216),
    "date" VARCHAR(16777216),
    "time" VARCHAR(16777216)
    );
    """
    sql_statements.append(create_table_chunked_pdf_dif)
    
    create_table_summarized_dif = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{summarized_content_dif} (
    "file_name" VARCHAR(16777216),
    SUMMARIZED_CHUNK VARCHAR(16777216),
    "chunks" VARCHAR(16777216)
);
"""
    sql_statements.append(create_table_summarized_dif)

    create_table_chunked_pdf_sum = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{chunked_pdf_sum} (
    "chunks" VARCHAR(16777216),
    "file_path" VARCHAR(16777216),
    "file_name" VARCHAR(16777216),
    "date" VARCHAR(16777216),
    "time" VARCHAR(16777216)
    );
    """
    sql_statements.append(create_table_chunked_pdf_sum)

    create_table_summarized_sum = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{summarized_content_sum} (
    "file_name" VARCHAR(16777216),
    SUMMARIZED_CHUNK VARCHAR(16777216),
    "chunks" VARCHAR(16777216)
);
"""
    sql_statements.append(create_table_summarized_sum)
    
    
    for smt in sql_statements:
        print(smt)
        conn.cursor().execute(smt)

if __name__ == "__main__":
    main()
