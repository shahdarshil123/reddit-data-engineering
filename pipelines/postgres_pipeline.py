from etls.postgres_etl import create_table,load_data_postgres,get_data
import os


def postgres_pipeline(file_name):
    create_table()
    print(os.path.dirname(os.path.dirname(__file__))+f'/data/{file_name}')
    load_data_postgres(os.path.dirname(os.path.dirname(__file__))+f'/data/{file_name}')
    get_data()

