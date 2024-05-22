from etls.reddit_etl import connect_reddit,extract_posts
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd


def reddit_pipeline(subreddit: str, time_filter='day', limit=None):
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'darshilshah622')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    print(post_df)

    print("Returning from reddit_pipeline module")
    return()
