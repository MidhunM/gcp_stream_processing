from google.cloud import pubsub_v1
import pandas as pd
import json

project_id = ""
topic_id = "stock_topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


df = pd.read_csv("indexProcessed.csv")

while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    print(dict_stock)
    future = publisher.publish(topic_path, json.dumps(dict_stock).encode('utf-8'))
    print(future.result())
    break

print(f"Published messages to {topic_path}.")