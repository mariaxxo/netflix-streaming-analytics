import os
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()  # secrets w .env

df = pd.read_excel("../data/netflix_sample.xlsx")  # sample dataset

connection_str = os.getenv("EVENTHUB_CONN_STR")
eventhub_name = os.getenv("EVENTHUB_NAME")

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)

with producer:
    batch = producer.create_batch()
    for i, row in df.iterrows():
        data = {
            "user_id": row["user_id"],
            "movie_id": row["movie_id"],
            "timestamp": str(row["timestamp"]),
            "country": row["country"],
            "genre": row["genre"],
            "watch_time": row["watch_time"],
            "popularity": row["popularity"]
        }
        batch.add(EventData(str(data)))
    producer.send_batch(batch)
print("Events sent!")

