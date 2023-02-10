import pandas as pd
import sqlite3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import time

# import warnings
# warnings.filterwarnings("ignore", category=InsecureRequestWarning)


conn = sqlite3.connect("/home/harsha/Documents/Movie-Reccomendation-App/database_v2/movies.sqlite")
cursor = conn.cursor()

query = """
SELECT *
FROM tweets_wol tw
WHERE id NOT in (SELECT id from newtweets_wl)
order by id
"""
query1 = """
SELECT *
FROM tweets_wol tw
WHERE id > 50400
"""

df = pd.read_sql_query(query, conn)


import threading
from queue import Queue

def get_redirected_url(url, q):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1)
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

sql_query = """
CREATE TABLE IF NOT EXISTS newtweets_wl(
    conversation_id VARCHAR(200),
    tweet VARCHAR(200),
    imdb_links VARCHAR(200),
    username VARCHAR(200),
    date VARCHAR(200),
    user_id VARCHAR(200),
    unix_time VARCHAR(200),
    id INTEGER
)
"""

cursor.execute(sql_query)
print("Opened database successfully")
chunk_size = 10000
total_urls = df.shape[0]
for i in range(0, total_urls, chunk_size):
    start_index = i
    end_index = i + chunk_size
    if end_index > total_urls:
        end_index = total_urls

    imdb_links = df["imdb_links"][start_index:end_index].values.tolist()
    fixed_imdb_links = []
    for link in imdb_links:
        if link is not None and not link.startswith(("http://", "https://")):
            fixed_imdb_links.append("http://" + link)
        else:
            fixed_imdb_links.append(link)


    redirected_urls = get_redirected_urls(fixed_imdb_links)

    print(f'Length of redirected_urls: {len(redirected_urls)}')
    print(f'Length of current chunk: {df[start_index:end_index].shape[0]}')

    # for j, row in df[start_index:end_index].iterrows():
    #     if row['imdb_links'] is not None:
    #         redirected_url = redirected_urls[j - start_index]
    #         df.at[j, 'imdb_links'] = redirected_url
    try:
        for j, row in df[start_index:end_index].iterrows():
            if row['imdb_links'] is not None:
                if j - start_index < len(redirected_urls):
                    redirected_url = redirected_urls[j - start_index]
                    df.at[j, 'imdb_links'] = redirected_url
    except:
        conn.close()
        print("Close database successfully")


    df[start_index:end_index].to_sql("newtweets_wl", conn, index=False, if_exists='append')
    print("ok")
    time.sleep(5)

# chunk_size = 1000
# total_urls = df.shape[0]
# for i in range(0, total_urls, chunk_size):
#     start_index = i
#     end_index = i + chunk_size
#     if end_index > total_urls:
#         end_index = total_urls
#
#     imdb_links = df["imdb_links"][start_index:end_index].values.tolist()
#     fixed_imdb_links = []
#     for link in imdb_links:
#         if link is not None and not link.startswith(("http://", "https://")):
#             fixed_imdb_links.append("http://" + link)
#         else:
#             fixed_imdb_links.append(link)
#
#     redirected_urls = get_redirected_urls(fixed_imdb_links)
#
#     for j, row in df[start_index:end_index].iterrows():
#         if row['imdb_links'] is not None:
#             redirected_url = redirected_urls[j - start_index]
#             df.at[j, 'imdb_links'] = redirected_url
#
#     df[start_index:end_index].to_sql("newtweets_wl", conn, index=False, if_exists='append')
#     print("ok")
#     time.sleep(5)


# total_urls = df.shape[0]
# for i in range(0, total_urls, chunk_size):
#     start_index = i
#     end_index = i + chunk_size
#     if end_index > total_urls:
#         end_index = total_urls
#
#     # redirected_urls = get_redirected_urls(df["imdb_links"][start_index:end_index].values.tolist())
#     # redirected_urls.to_sql("newtweets_wl", conn, index=False, if_exists='append')
#     df['imdb_links'] = df['imdb_links'].apply(lambda x: get_redirected_urls(x) if x is not None else None)
#     df.to_sql("newtweets_wl", conn, index=False, if_exists='append')

conn.close()
print("Closed database successfully")
