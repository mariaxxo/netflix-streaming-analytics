import os
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from datetime import datetime
import random
import json

load_dotenv()  # wczytuje secrets z .env

df = pd.read_excel("../data/netflix_sample.xlsx")  # Twój dataset filmów

connection_str = os.getenv("EVENTHUB_CONN_STR")
eventhub_name = os.getenv("EVENTHUB_NAME")

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)

with producer:
    batch = producer.create_batch()
    for i, row in df.iterrows():
        user_id = f"user_{random.randint(1,200)}"
        ts = datetime.now() - pd.to_timedelta(random.randint(0, 24*60), unit='m')
        country = random.choice(["US", "UK", "FR", "DE", "PL"])
        max_duration = row.get("duration", 120)  
        watch_time = random.randint(5, max_duration)
        
        data = {
            "user_id": user_id,
            "movie_title": row["title"],  
            "timestamp": ts.isoformat(),
            "country": country,
            "genre": row["genre"],
            "watch_time": watch_time,
            "rating": row["rating"]
        }
        
        batch.add(EventData(json.dumps(data)))
    producer.send_batch(batch)

print("Events sent!")
