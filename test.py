import config
import requests

url = f'http://www.omdbapi.com/?i=tt3896198&apikey={config.OMDB_API_KEY}'
response = requests.get(url)
data = response.json()

print(data)
