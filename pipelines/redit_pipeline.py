from etls.reddit_etl import connect_reddit,extract_posts, transform_data,load_data_to_csv, create_insert_script
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd
import os


def reddit_pipeline(file_name: str,subreddit: str, time_filter='day', limit=None):
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'darshilshah622')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    # print(posts)
    post_df = pd.DataFrame(posts)
    # print(post_df)
    # transformation
    post_df = transform_data(post_df)
    
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    insert_sql_path =  f'{os.path.dirname(os.path.dirname(__file__))}/dags/sql/insert_data.sql'
    # file_path = f'{os.path.dirname(os.path.dirname(__file__))}/data/{file_name}.csv'
    # print(file_path)
    load_data_to_csv(post_df, file_path)
    # create_insert_script(file_path,insert_sql_path)
    
    

