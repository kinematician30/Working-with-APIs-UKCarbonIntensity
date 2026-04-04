import asyncio
import aiohttp
import os
import json
from datetime import date, timedelta

# Bronze folder for raw data
BRONZE_PATH = "data/bronze"
os.makedirs(BRONZE_PATH, exist_ok=True)

async def fetch_carbon_data(session, target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    file_path = f"{BRONZE_PATH}/{target_date}.json"
    
    if os.path.exists(file_path):
        return # skip if bronze exists

    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                with open(file_path, 'w') as f:
                    json.dump(data['data'], f)
                print(f"Downloaded: {target_date}")
            else:
                print(f"Failed {target_date}: Status {response.status}")
    except Exception as e:
        print(f"Error on {target_date}: {e}")

async def run_extraction():
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)
    current = start_date
    
    tasks = []
    # Apply rate limiting to reduce simultaneous connections (this ultimately prevents IP bans)
    connector = aiohttp.TCPConnector(limit=5) 
    async with aiohttp.ClientSession(connector=connector) as session:
        while current <= end_date:
            tasks.append(fetch_carbon_data(session, current))
            current += timedelta(days=1)
        await asyncio.gather(*tasks)

# To run: 
asyncio.run(run_extraction())