import sys
import csv
import psycopg2
import os

db_params = {
        'dbname': 'airflow_reddit',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'postgres',
        'port': '5432'
    }

def get_data():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    #create Reddit table in PostgresSQL database
    cursor.execute('SELECT COUNT(*) FROM reddit')
    result = cursor.fetchall()[0][0]
    print(result)
    cursor.close()
    conn.close()
    

def create_table():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    #create Reddit table in PostgresSQL database
    create_table_query ='''CREATE TABLE IF NOT EXISTS Reddit(
        id VARCHAR(10) PRIMARY KEY,
        title TEXT,
        score INT,
        num_comments INT,
        author VARCHAR(50),
        created_utc TIMESTAMP,
        url TEXT,
        over_18 BOOLEAN,
        edited BOOLEAN,
        spoiler BOOLEAN,
        stickied BOOLEAN
    );'''
    cursor.execute(create_table_query)
    conn.commit()

    print("Table created/ found successfully")

    cursor.close()
    conn.close()

def load_data_postgres(file_name):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    with open(file_name, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        for row in reader:
            try:
                cursor.execute(
                    "INSERT INTO Reddit (id, title, score, num_comments, author, created_utc, url, over_18, edited, spoiler, stickied) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    row
                )
            except Exception as e:
                continue
    

    # Commit the transaction
    conn.commit()
    print("All rows inserted!")
    # Close the cursor and connection
    cursor.close()
    conn.close()

def remove_all_rows():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    #create Reddit table in PostgresSQL database
    cursor.execute('DROP TABLE Reddit')
    conn.commit()

    print("Table Truncated")

    cursor.close()
    conn.close()


# create_table()
# get_data()
# load_data_postgres(os.path.dirname(os.path.dirname(__file__))+'/data/reddit_2024052303.csv')
# get_data()






