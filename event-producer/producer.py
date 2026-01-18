import os
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from datetime import datetime
import random
import json
import time

load_dotenv()  # wczytuje secrets z .env

df = pd.read_csv("../data/sample_netflix1.csv")

connection_str = os.getenv("EVENTHUB_CONN_STR")
eventhub_name = os.getenv("EVENTHUB_NAME")

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)

trending_genres = ["Action", "Comedy", "Drama"]

with producer:
    print("Rozpoczynam symulację ruchu użytkowników...")
    
    for _ in range(1000):
        random_row = df.sample(n=1).iloc[0]
        
        genre = str(random_row["genre"]).split(',')[0].strip()
        if random.random() > 0.7: 
            genre = random.choice(trending_genres)
        
        event = {
            "user_id": f"user_{random.randint(1, 100)}", 
            "movie_title": str(random_row["title"]),
            "timestamp": datetime.now().isoformat(),
            "country": random.choice(["PL", "US", "UK"]),
            "genre": genre,
            "watch_time": random.randint(1, 120),
            "rating": str(random_row["rating"])
        }
        
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)
        
        time.sleep(0.1)

print("Events sent!")