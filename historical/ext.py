import asyncio
import aiohttp
import os
import json
from datetime import date, timedelta

BRONZE_DIR = "data/bronze"
os.makedirs(BRONZE_DIR, exist_ok=True)

# Limit to 3 concurrent requests to avoid being banned by the API
semaphore = asyncio.Semaphore(3)

async def fetch_day(session, target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    file_path = os.path.join(BRONZE_DIR, f"{target_date}.json")
    
    if os.path.exists(file_path): 
        return # Skip if we already have it

    async with semaphore:
        for attempt in range(3): # Try 3 times
            try:
                async with session.get(url, timeout=20) as response:
                    if response.status == 200:
                        res_json = await response.json()
                        with open(file_path, "w") as f:
                            json.dump(res_json["data"], f)
                        print(f"✅ Saved: {target_date}")
                        return
                    elif response.status == 429:
                        print(f"⚠️ Rate limited on {target_date}. Waiting...")
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        print(f"❌ Status {response.status} for {target_date}")
            except Exception as e:
                print(f"🔄 Retry {attempt+1} for {target_date} due to {e}")
                await asyncio.sleep(2)
        print(f"🚫 FAILED after 3 attempts: {target_date}")

async def run_history():
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)
    
    # TCPConnector helps prevent 'Max retries' errors
    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        curr = start_date
        while curr <= end_date:
            tasks.append(fetch_day(session, curr))
            curr += timedelta(days=1)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_history())