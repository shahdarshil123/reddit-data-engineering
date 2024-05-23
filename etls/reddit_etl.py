import sys

import numpy as np
import pandas as pd
from praw import Reddit
import json
import csv
import datetime


from utils.constants import POST_FIELDS


def connect_reddit(client_id, client_secret, user_agent) -> 'Reddit':
    try:
        reddit = Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    post_lists = []
    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)
    # print(post_lists)
    return post_lists

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
    

def create_insert_script(csv_file_name, insert_sql_file_name):
    with open(csv_file_name, 'r') as csvfile, open(insert_sql_file_name,'w') as outputfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            # print(row)
            id = row[0]
            title = row[1]
            score = int(row[2])
            num_comments = int(row[3])
            author = row[4]
            created_utc = row[5]
            url = row[6]
            over_18 = row[7]
            edited = row[8]
            spoiler = row[9]
            stickied = row[10]

            # Generate the INSERT statement
            insert_statement = f"""
            INSERT INTO reddit_posts (id, title, score, num_comments, author, created_utc, url, over_18, edited, spoiler, stickied) 
            VALUES ("{id}", "{title}", {score}, {num_comments}, "{author}", "{created_utc}", "{url}", {over_18}, {edited}, {spoiler}, {stickied});
            """
            outputfile.write(insert_statement)
        print("insert statements complested")
    





