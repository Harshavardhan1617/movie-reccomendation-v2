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

sql_query = """
SELECT *
from tweets t
WHERE imdb_links IS NULL AND tweet LIKE '%/t.co%'
GROUP BY t.unix_time
"""

DATABASE_LOCATION = "sqlite:////media/harsha/workspace/projects/Movie-Reccomendation-App/database_v2/movies.sqlite"
#Connecting to sqlite
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('/media/harsha/workspace/projects/Movie-Reccomendation-App/database_v2/movies.sqlite')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

def extract_url(text):
    match = re.search(r'https?://\S+', text)
    if match:
        return match.group()
    return None


def get_redirected_url(url):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.head(url, allow_redirects=True)
    return response.url

df = pd.read_sql_query(sql_query, conn)
print(df.info())

df['imdb_links'] = df['tweet'].apply(lambda x: extract_url(x))
url_start = now0.strftime("%d/%m/%Y %H:%M:%S")
print(url_start)
df['imdb_links'] = df['imdb_links'].apply(lambda x: get_redirected_url(x) if x is not None else None)

print("after : ", df.info())

# Append the data to the existing table in the database
df.to_sql("tweets", engine, index=False, if_exists='append', method='multi')

from datetime import datetime
print("start =", dt_string0)

# datetime object containing current date and time
now = datetime.now()

#print("now =", now)

# dd/mm/YY H:M:S
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
print("end =", dt_string)
