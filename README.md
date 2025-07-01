# Real-time Customer Behavior Analytics

  Big Data using:
- Kafka + Zookeeper
- Spark Structured Streaming
- MongoDB
- Power BI (Push API)

## Components

- `kafka_producer.py`: Generates mock data, pushes it to Kafka, and stores it in MongoDB.
- `spark_streaming.py`: Processes data from Kafka and stores it in MongoDB and Parquet.
- `push_to_powerbi.py`: Sends data to the Power BI REST API.
- `docker-compose.yml`: Easily run everything in an integrated environment.
