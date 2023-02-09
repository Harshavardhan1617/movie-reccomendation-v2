import sqlalchemy
import pandas as pd
import re
#import json
import datetime
from sqlalchemy.orm import sessionmaker
import sqlite3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import twint
import time

while True:

    from datetime import datetime
    now0 = datetime.now()
    # dd/mm/YY H:M:S
    dt_string0 = now0.strftime("%d/%m/%Y %H:%M:%S")


    DATABASE_LOCATION = "sqlite:///movies.sqlite"
    #Connecting to sqlite
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('movies.sqlite')
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()


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


    # #extract
    # c = twint.Config()
    # c.Search = "I rated* /10 #IMDb"
    # c.Custom = ["conversation_id", "created_at","tweet", "username", "date", "user_id"]
    # c.Until = old_date
    # c.Limit = 225
    # c.Pandas = True

    try:
        old_date = cursor.execute('''
        SELECT strftime('%Y-%m-%d %H:%M:%S', MIN(date)) as date
        FROM tweets

        ''')
        old_date = cursor.fetchone()[0];
        print("the oldest date in database is : ", old_date)

    #extract
        c = twint.Config()
        c.Search = "I rated* /10 #IMDb"
        c.Custom = ["conversation_id", "created_at","tweet", "username", "date", "user_id"]
        c.Until = old_date
        c.Limit = 2500
        c.Pandas = True


        twint.run.Search(c)
        tweets_df = twint_to_pd(["conversation_id","tweet", "username", "date", "user_id"])
    except:

        updated_date = cursor.execute('''
        SELECT strftime('%Y-%m-%d', MIN(date)) as date
        FROM tweets
        ''')
        # from datetime import datetime, timedelta
        # date_format = '%Y-%m-%d'
        # #old_date = '2022-12-15 00:00:00'
        # updated_date = datetime.strptime(old_date, date_format)
        # u_date = updated_date - timedelta(days=2)
        updated_date = cursor.fetchone()[0];
        print("updated date is :", updated_date)

        c2 = twint.Config()
        c2.Search = "I rated* /10 #IMDb"
        c2.Custom = ["conversation_id", "created_at","tweet", "username", "date", "user_id"]
        c2.Until = updated_date

        c2.Limit = 2500
        c2.Pandas = True
        twint.run.Search(c2)
        tweets_df = twint_to_pd(["conversation_id","tweet", "username", "date", "user_id"])



    tweets_df = twint_to_pd(["conversation_id","tweet", "username", "date", "user_id"])
    #tweets_df.to_csv('tweets.csv', index = False)

    #df = tweets_df
    #tweets_df.loc[:, 'tweet'] = tweets_df['tweet'].str.split("#IMDb", expand=True)[0][:-1]
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
    # tweets_df['imdb_links'] = tweets_df['imdb_links'].apply(lambda x: get_redirected_urls(x) if x is not None else None)
    # imdb_links = tweets_df['imdb_links'].fillna('').tolist()
    # redirected_urls = get_redirected_urls(imdb_links)
    #
    # for i, redirected_url in enumerate(redirected_urls):
    #     tweets_df.loc[i, 'imdb_links'] = redirected_url


    if check_if_valid_data(tweets_df):
        print("Data valid, proceed to Load stage")

    # Load
    #
    # engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    # conn = sqlite3.connect('movies.sqlite')
    # cursor = conn.cursor()

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


    cursor.execute(sql_query)
    print("Opened database successfully")

    tweets_df.to_sql("tweets", engine, index=False, if_exists='append')
    #
    # try:
    #     tweets_df.to_sql("tweets", engine, index=False, if_exists='append')
    # except:
    #     print("Data already exists in the database")

    conn.close()
    print("Close database successfully")


    # cursor = conn.cursor()
    # q_extract = """
    # SELECT ROWID as S_No
    # ,user_id
    # ,username
    # ,tweet
    # ,unix_time
    # from tweets
    # WHERE unix_time>(SELECT MAX(unix_time) FROM tweet_titles)
    #
    # """
    #
    # try:
    #     df = pd.read_sql_query(q_extract, engine)
    #     print(df.info())
    # except:
    #     print("failed to load data")
    #
    # #dropping null values
    # df = df.dropna()
    # print("#after dropping null \n", df.info())
    #
    # #extracting titles,year and ratings from tweets
    # df[['title','year','rating']] = df['tweet'].str.extract(r'rated (.*) \((.*)\) (.*)')
    # print("#after extracting strings", df.info())
    #
    # #removing null titles
    # df_2 = df[df['title'].isnull()]
    # df = df[~df['S_No'].isin(df_2['S_No'])]
    #
    # #removing tv-shows from titles
    # df_1 = df[df['year'].str.len()>4]
    # df = df[~df['S_No'].isin(df_1['S_No'])]
    # print("#after removing tv-shows", df.info())
    #
    # df = df.drop('S_No', axis=1)
    # df.info()
    #
    # #load
    # sql_query_2 = """
    # CREATE TABLE IF NOT EXISTS tweet_titles(
    #     user_id VARCHAR(200),
    #     username VARCHAR(200),
    #     tweet VARCHAR(200),
    #     unix_time VARCHAR(200),
    #     title VARCHAR(200),
    #     year VARCHAR(200),
    #     rating VARCHAR(200)
    #
    #
    # )
    # """
    #
    #
    # cursor.execute(sql_query_2)
    # print("Opened tweet_titles table successfully")
    #
    # try:
    #     df.to_sql("tweet_titles", engine, index=False, if_exists='append')
    # except:
    #     print("Data already exists in the database")
    #
    # conn.close()
    # print("Close database successfully")

    from datetime import datetime
    print("start =", dt_string0)

    # datetime object containing current date and time
    now = datetime.now()

    #print("now =", now)

    # dd/mm/YY H:M:S
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("end =", dt_string)

    time.sleep(10)
