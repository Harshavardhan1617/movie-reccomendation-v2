import sqlite3
import pandas as pd

conn = sqlite3.connect("movies.sqlite")
cursor = conn.cursor()

query = """
SELECT DISTINCT user_id as twitter_id
from tweets t
"""
df = pd.read_sql_query(query, conn)

df.index += 1
df.index.name = "user_id"

df.to_sql("users", conn, if_exists="replace")

conn.close()
