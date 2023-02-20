from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage
import json


project_id      = ""
subscription_id = "stock_topic-sub"
timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)



def write_read(message):


    bucket_name = "pub_sub_gcp_bucket"
    blob_name   = "stock_data.json"
    data = json.loads(message.data.decode('utf-8'))

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(json.dumps(data))

    message.ack()

    print("Success.................................................")


streaming_pull_future = subscriber.subscribe(subscription_path, callback=write_read)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  
        streaming_pull_future.result()  