import requests
import asyncio


async def get_full_url(url):
    async with requests.head(url, allow_redirects=True) as response:
        return response.url

async def main():
    full = await get_full_url("https://t.co/ULWDovA0pp")
    print(full)

asyncio.run(main())
