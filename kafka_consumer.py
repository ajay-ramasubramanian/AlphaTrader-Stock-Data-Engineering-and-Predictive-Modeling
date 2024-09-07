from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import json
# from spotify_user_data_extraction import users_saved_tracks

def consumer(topic='quickstart-events', bootstrap_servers=['localhost:9092'], 
               group_id='dataframe_consumer_group'):
    # Create a Kafka consumer
    kafka_consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: x.decode('utf-8')
    )
    def process_dataframe(df):
        print("Received DataFrame:")
        print(df)
        print("------------------------")
    try:
            for message in kafka_consumer:
                # print(f"Received raw message: {message.value}")
                try:
                    # Try to parse the message as JSON
                    data = json.loads(message.value)
                    # Convert the message value (list of dicts) back to a DataFrame
                    df = pd.DataFrame(data)
                    # Process the DataFrame
                    process_dataframe(df)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    print(f"Problematic message: {message.value[:100]}...")  # Print first 100 chars of the message
                except Exception as e:
                    print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        # Close the consumer
        kafka_consumer.close()

receive_df = consumer()