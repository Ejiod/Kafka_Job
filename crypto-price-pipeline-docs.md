# Crypto Price Pipeline Documentation

## Overview

This project implements a real-time cryptocurrency price tracking pipeline using Apache Kafka for data ingestion and Apache Flink for stream processing. The pipeline fetches price data from the Binance API, publishes it to Kafka topics, and then processes and stores the data in a PostgreSQL database.

## Components

1. BTC_Kafka.py: Kafka Producer
2. BTC_Flink.py: Flink Stream Processor

### 1. BTC_Kafka.py

This script acts as a Kafka producer, fetching cryptocurrency price data from the Binance API and publishing it to Kafka topics.

#### Dependencies

- kafka-python
- python-binance

#### Configuration

- Binance API credentials (api_key and api_secret)
- Kafka bootstrap server (localhost:9092)
- Symbol-topic mapping:
  - BTCUSDT -> BTC
  - AVAXUSDT -> AVA
  - BNBUSDT -> BNB
  - SOLUSDT -> SOL

#### Functionality

1. Initializes a Binance client and a Kafka producer.
2. Continuously fetches price data for the configured symbols.
3. Publishes price data to the corresponding Kafka topics.
4. Data format: JSON with fields 'symbol', 'price', and 'event_time_ms'.
5. Runs in an infinite loop with a 0.5-second delay between iterations.

### 2. BTC_Flink.py

This script sets up an Apache Flink job to consume data from Kafka topics, process it, and store it in a PostgreSQL database.

#### Dependencies

- pyflink
- Kafka connector JAR
- PostgreSQL JDBC driver
- Flink JDBC connector

#### Configuration

- Flink execution environment with parallelism set to 1
- Kafka source tables for topics: BTC, SOL, BNB
- PostgreSQL sink table

#### Functionality

1. Creates Flink execution and table environments.
2. Defines Kafka source tables for each cryptocurrency topic.
3. Defines a PostgreSQL sink table.
4. Implements a processing query that:
   - Combines data from all Kafka topics
   - Converts Unix timestamp to a proper timestamp
   - Filters out potentially invalid timestamps
   - Inserts processed data into the PostgreSQL sink table
5. Executes the Flink job to continuously process incoming data

## Data Flow

1. BTC_Kafka.py fetches price data from Binance API.
2. Price data is published to respective Kafka topics (BTC, AVA, BNB, SOL).
3. BTC_Flink.py consumes data from Kafka topics (BTC, SOL, BNB).
4. Flink job processes and combines the data streams.
5. Processed data is inserted into the PostgreSQL database.

## Database Schema

The PostgreSQL table `machine.crypto_prices` has the following schema:

- symbol: STRING (Primary Key part 1)
- price: DOUBLE
- event_time: TIMESTAMP(3)
- topic: STRING (Primary Key part 2)

## Notes

- Ensure all required JAR files are in the correct location for the Flink job.
- The Flink job is configured to start consuming from the latest offset in Kafka topics.
- The event_time_ms filter (> 1000000000000) is used to exclude potentially invalid timestamps.
- The PostgreSQL connection details (URL, username, password) should be updated as per your setup.

## Future Improvements

1. Implement error handling and logging in both scripts.
2. Add configuration files to externalize settings.
3. Implement data validation and cleansing in the Flink job.
4. Consider adding monitoring and alerting for the pipeline.
5. Optimize Flink job for better performance and scalability.
