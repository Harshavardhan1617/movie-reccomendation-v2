import sqlalchemy
import pandas as pd
import re
#import json
import datetime
from sqlalchemy.orm import sessionmaker
import sqlite3
import requests
import twint

from datetime import datetime
now0 = datetime.now()
# dd/mm/YY H:M:S
dt_string0 = now0.strftime("%d/%m/%Y %H:%M:%S")


         #for ubuntu
# DATABASE_LOCATION = "sqlite:////home/harsha/Documents/Movie-Reccomendation-App/database_v2/movies.sqlite"
# #Connecting to sqlite
# engine = sqlalchemy.create_engine(DATABASE_LOCATION)
# conn = sqlite3.connect('/home/harsha/Documents/Movie-Reccomendation-App/database_v2/movies.sqlite')
#Creating a cursor object using the cursor() method

        #for wsl
DATABASE_LOCATION = "sqlite:////mnt/h/'My Drive'/Databases/movies.sqlite"
#Connecting to sqlite
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('/mnt/h/'My Drive'/Databases/movies.sqlite')
#cursor
cursor = conn.cursor()

#functions
def twint_to_pd(columns):
    return twint.output.panda.Tweets_df[columns]

def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No tweets downloaded")
        return False


# def remove_non_english_letters(text):
#     return re.sub(r'[^a-zA-Z0-9!@#$%^&*()_+-={}|[]\:";<>,.?/~`]', '', text)
def remove_non_ascii_2(string):
    return string.encode('ascii', errors='ignore').decode()

def extract_url(text):
    match = re.search(r'https?://\S+', text)
    if match:
        return match.group()
    return None

# def get_redirected_url(url):
#     response = requests.head(url, allow_redirects=True)
#     return response.url


import threading
from queue import Queue

def get_redirected_url(url, q):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.head(url, allow_redirects=True)
    q.put(response.url)

def get_redirected_urls(urls):
    q = Queue()
    threads = []
    for url in urls:
        thread = threading.Thread(target=get_redirected_url, args=(url, q))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    return [q.get() for _ in range(q.qsize())]

#functions

try:
    new_date = cursor.execute('''
    SELECT strftime('%Y-%m-%d %H:%M:%S', MAX(date)) as date
    FROM tweets

    ''')
    new_date = cursor.fetchone()[0];
    print("the newest date in database is : ", new_date)

    #extract
    c = twint.Config()
    c.Search = "I rated* /10 #IMDb"
    c.Custom = ["conversation_id", "created_at","tweet", "username", "date", "user_id"]
    c.Since = new_date
    c.Limit = 2500
    c.Pandas = True
    twint.run.Search(c)
    tweets_df = twint_to_pd(["conversation_id","tweet", "username", "date", "user_id"])

except:
    new_date = cursor.execute('''
    SELECT strftime('%Y-%m-%d', MAX(date)) as date
    FROM tweets

    ''')
    new_date = cursor.fetchone()[0];
    print("the newest date in database is : ", new_date)

    #extract
    c = twint.Config()
    c.Search = "I rated* /10 #IMDb"
    c.Custom = ["conversation_id", "created_at","tweet", "username", "date", "user_id"]
    c.Since = new_date
    c.Limit = 2500
    c.Pandas = True
    twint.run.Search(c)
    tweets_df = twint_to_pd(["conversation_id","tweet", "username", "date", "user_id"])

#pandas operations
tweets_df['tweet'] = tweets_df['tweet'].str.replace('.*I rated', 'I rated')
# tweets_df['tweet'] = tweets_df['tweet'].apply(remove_non_english_letters)
tweets_df['tweet'] = tweets_df['tweet'].apply(remove_non_ascii_2)
tweets_df['tweet'] = tweets_df['tweet'].apply(lambda x: re.sub(" +", " ", x))
tweets_df["date"] = pd.to_datetime(tweets_df["date"])
tweets_df["date"] = tweets_df["date"].dt.strftime('%Y-%m-%d %H:%M:%S')
tweets_df["unix_time"] = pd.to_datetime(tweets_df["date"]).astype(int) / 10**9
tweets_df['imdb_links'] = tweets_df['tweet'].apply(lambda x: extract_url(x))
url_start = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
print(url_start)

if check_if_valid_data(tweets_df):
    print("Data valid, proceed to Load stage")

sql_query = """
CREATE TABLE IF NOT EXISTS tweets(
    conversation_id VARCHAR(200),
    tweet VARCHAR(200),
    imdb_links VARCHAR(200),
    username VARCHAR(200),
    date VARCHAR(200),
    user_id VARCHAR(200),
    unix_time VARCHAR(200)
)
"""


# cursor.execute(sql_query)
# print("Opened database successfully")
# tweets_df.to_sql("tweets", engine, index=False, if_exists='append')
# conn.close()
# print("Close database successfully")

tweets_df.to_csv('new_tweets.csv')


#after closing database
from datetime import datetime
print("start =", dt_string0)
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
print("end =", dt_string)
