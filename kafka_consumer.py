from kafka import KafkaConsumer
import pandas as pd
import json
from spotify_user_data_extraction import *

def consumer(topic='quickstart', bootstrap_servers=['localhost:9092'], group_id='dataframe_consumer_group'):
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    def process_dataframe(df):
        # This is where you can process the DataFrame
        # For this example, we'll just print it
        print("Received DataFrame:")
        print(df)
        print("------------------------")

    # Consume messages
    try:
        for message in consumer:
            # Convert the message value (list of dicts) back to a DataFrame
            df = pd.DataFrame(message.value)
            
            # Process the DataFrame
            process_dataframe(df)

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        # Close the consumer
        consumer.close()

