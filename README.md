
# Spotify Insight: End-to-End Data Engineering & ML pipeline

### Project Overview
This project creates an end-to-end data pipeline that replicates Spotify Wrapped functionality, allowing users to view their personalized music insights at any time. The pipeline extracts data from the Spotify Web API, processes it using modern data engineering technologies, and presents user-friendly visualizations.

### Architecture

The data pipeline consists of the following components:

- Data Extraction: Spotify Web API
- Message Broker/Event Streaming: Apache Kafka
- Data Processing: Apache Spark
- Data Lake: MinIO( S3 compatible Object Store )
- Orchestration: Apache Airflow
- Data Warehouse: Amazon Redshift
- Visualization: PowerBI

### Apache Kafka Workflow
![kafka workflow](images/kafka_data_flow.png)

The system is designed for high scalability, capable of serving millions of concurrent users. Each Kafka topic is partitioned to efficiently manage thousands of users per topic. To accommodate growing user bases, we can horizontally scale by deploying additional Kafka brokers with replicated topics. This architecture ensures seamless performance and data throughput even as user numbers increase dramatically.

### Features
- Daily data extraction from Spotify Web API
- Processing of user listening history, top tracks, and artists
- Calculation of listening trends and preferences
- Interactive dashboard for viewing personalized insights
- Scalable architecture to handle millions of users

### :emoji: Setup and Installation ( Stage 1 in development)

Clone the repository :simle:

- Install required dependencies:`pip install -r requirements.txt`
- To run Kafka and MinIO object store run: `docker-compose up -d`
- Run the kafka_consumer.py file to start the consumer.
- Then, you can run all the kafka_produce files simultaneously and see all the data written to your MinIO bucket.

### Scaling Considerations

### Future Enhancements

### License
This project is licensed under the MIT License.