# Real-Time Stock Market Data Engineering Pipeline

**Status: In Progress**

## Project Overview

This project is a real-time data engineering pipeline designed to ingest, stream, process, validate, store, and query live US stock market data.

Live trade data is collected from the Finnhub WebSocket API using a Python producer running on a local Windows machine. The producer normalizes incoming trade records and publishes them to Apache Kafka hosted on an AWS EC2 instance.

A separate AWS EC2 instance runs Apache Spark Structured Streaming, which consumes Kafka messages through the private AWS network, parses the JSON payload, applies schema validation and data-quality checks, and prepares the data for storage in Amazon S3.

The final architecture will use Amazon S3 as the data lake, AWS Glue as the metadata catalog, and Amazon Athena as the SQL query layer for downstream analysts and data scientists.

The project follows a Kappa-style streaming architecture, with optional bronze, silver, and gold data zones for organizing raw, validated, and aggregated datasets.

```text
Finnhub WebSocket API
        ↓
Local Python Producer
        ↓
Apache Kafka on AWS EC2
        ↓
Apache Spark Structured Streaming on AWS EC2
        ↓
Amazon S3
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena
```

## What Has Been Accomplished

* Created an AWS EC2 instance to host Apache Kafka and ZooKeeper.

* Installed and configured Kafka with separate internal and external listeners.

* Configured the internal Kafka listener on port `9092` for communication between AWS EC2 instances.

* Configured the external Kafka listener on port `9093` for communication from the local Windows machine.

* Updated Kafka `advertised.listeners` to support both private and public network access.

* Configured AWS security-group rules for SSH, local producer access, and private Spark-to-Kafka communication.

* Diagnosed severe Kafka latency and SSH freezing while using a `t3.micro` EC2 instance.

* Upgraded the Kafka instance to a larger instance type, which resolved producer, consumer, and SSH latency issues.

* Added a Linux swapfile to the Kafka EC2 instance for memory overflow protection.

* Created the Kafka topics:

```text
stock-trades-test
stock-trades-raw
```

* Configured both topics with three Kafka partitions and a replication factor of one.

* Built a local Python Kafka producer using `confluent-kafka`.

* Created a simulated stock-trade generator for testing Kafka connectivity before connecting to Finnhub.

* Added Kafka delivery callbacks to confirm successful message delivery, partition assignment, and offsets.

* Used stock symbols as Kafka message keys to preserve per-symbol ordering within Kafka partitions.

* Successfully sent test JSON messages from the Windows producer to Kafka on AWS EC2.

* Verified near-zero Kafka delivery latency during local producer testing.

* Created a Finnhub API account and configured the API key through environment variables.

* Created `.env` and `.env.example` files for environment-specific configuration.

* Protected sensitive configuration such as the Finnhub API key through `.gitignore`.

* Verified Finnhub REST API access using the stock quote endpoint.

* Created a Finnhub WebSocket client using Python.

* Successfully connected to the Finnhub WebSocket API.

* Subscribed to live trade data for:

```text
AAPL
MSFT
NVDA
TSLA
AMZN
```

* Built a live Finnhub Kafka producer that receives trade batches from the WebSocket.

* Normalized Finnhub’s abbreviated trade fields into a clearer event schema.

* Added fields such as:

```text
schema_version
event_id
event_type
source
symbol
price
volume
trade_conditions
event_timestamp_ms
event_timestamp_utc
ingestion_timestamp_utc
```

* Added a unique UUID to every normalized trade event.

* Added ingestion timestamps to support pipeline-latency monitoring.

* Tested the Finnhub producer using simulated Finnhub-shaped messages while the market was closed.

* Successfully received real Finnhub trade events during US market hours.

* Verified that live Finnhub trade data was published to Kafka and consumed in real time.

* Created a separate AWS EC2 instance for Apache Spark Structured Streaming.

* Placed the Spark and Kafka EC2 instances inside the same AWS VPC.

* Configured private Kafka access from the Spark EC2 security group to port `9092`.

* Successfully tested the private connection from the Spark instance to Kafka using `nc`.

* Installed Java 17 on the Spark EC2 instance.

* Configured `JAVA_HOME` and Spark environment variables.

* Added a swapfile to the Spark EC2 instance.

* Installed Apache Spark with Hadoop support.

* Verified the Spark installation using the built-in Pi calculation example.

* Created a PySpark Structured Streaming consumer.

* Added the Spark Kafka connector through the `spark-sql-kafka` package.

* Successfully connected Spark Structured Streaming to Kafka.

* Read Kafka records from `stock-trades-raw` in real time.

* Converted Kafka keys and values from binary format into readable strings.

* Retained Kafka metadata including:

```text
topic
partition
offset
Kafka timestamp
message key
```

* Created an explicit Spark schema for trade events.

* Parsed Kafka JSON messages into individual Spark DataFrame columns.

* Configured the Spark session to use UTC timestamps.

* Converted event-time and ingestion-time strings into Spark timestamp columns.

* Added a Spark processing timestamp.

* Calculated producer latency between Finnhub event time and producer ingestion time.

* Identified minor clock differences between Finnhub timestamps and the local producer clock.

* Added data-quality validation for:

```text
Missing event IDs
Missing symbols
Unsupported symbols
Kafka key and symbol mismatches
Invalid prices
Invalid volumes
Invalid event timestamps
Invalid ingestion timestamps
```

* Added `is_valid` and `validation_error` columns.

* Successfully validated live trade records with:

```text
is_valid = true
validation_error = NULL
```

* Confirmed that Spark processes live Kafka data continuously using micro-batches.

* Preserved Kafka offsets and partitions for future auditing and troubleshooting.

## What Is Left / Next Steps

* Write valid trade records to the Amazon S3 bronze zone.

* Route invalid or malformed trade records to a separate quarantine path.

* Create time-based S3 partitions using year, month, day, and hour.

* Configure persistent Spark checkpoint locations.

* Test Spark recovery after application or EC2 restarts.

* Add IAM permissions for the Spark EC2 instance to write to S3.

* Store bronze data in JSON or Parquet format.

* Create a cleaned silver trade dataset.

* Add deduplication using event IDs and Kafka metadata.

* Add quote snapshot ingestion using the Finnhub REST API.

* Add company profile and stock-symbol reference datasets.

* Add company basic financial metrics.

* Add company news and general market news ingestion.

* Add earnings-calendar ingestion.

* Create one-minute OHLCV streaming aggregations.

* Store aggregated datasets in a gold S3 zone.

* Configure AWS Glue Crawlers.

* Register S3 datasets in the AWS Glue Data Catalog.

* Query trade and enrichment datasets using Amazon Athena.

* Create Athena validation queries for trade counts, latest prices, volume, and company metadata joins.

* Add structured logging and improved exception handling.

* Add retry and reconnection logic for the Finnhub WebSocket.

* Add monitoring for Kafka, Spark, API failures, and malformed events.

* Create an architecture diagram.

* Add screenshots, setup instructions, commands, and troubleshooting notes.

* Document the Kafka `t3.micro` performance bottleneck and EC2 upgrade as a real-world infrastructure lesson.

* Add an optional analytics dashboard after the data engineering pipeline is complete.
