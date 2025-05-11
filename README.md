# FlightAnalysis Lambda Architecture

This project implements a Lambda Architecture for analyzing flight data using Apache Spark, Kafka, and Elasticsearch. It consists of a batch layer for historical data processing, a speed layer for real-time data streaming, and a serving layer for visualization.

## Requirements

- **Docker** and **Docker Compose**: Install from [Docker official site](https://docs.docker.com/get-docker/).
- **Hadoop (HDFS)**: Version 3.x, installed and running. See [Hadoop setup guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).
- **Python**: Version 3.x, with a virtual environment recommended (`python -m venv env`).
- **Apache Kafka**: Managed via Docker (included in `docker-compose.yml`).
- **Elasticsearch**: Managed via Docker (included in `docker-compose.yml`).
- **Kibana**: Managed via Docker for visualization (included in `docker-compose.yml`).
- **Apache Spark**: Managed via Docker with Python support (PySpark).

## Dataset

The project uses flight data in CSV format (e.g., from Kaggle's flight dataset). To prepare the dataset:

1. Download the dataset from a source like [Kaggle Flights Data](https://www.kaggle.com/datasets).
2. Place the CSV files in a local directory (e.g., `./data/`).
3. Upload the files to HDFS:
   ```bash
   hdfs dfs -mkdir /flight-data
   hdfs dfs -put ./data/*.csv /flight-data
   ```

## Architecture

This project follows a Lambda Architecture with three layers:

- **Batch Layer**: Processes historical data using Spark.
- **Speed Layer**: Handles real-time data with Kafka and Spark Streaming.
- **Serving Layer**: Stores results in Elasticsearch and visualizes them with Kibana.

Data flows from raw CSV files or real-time streams to processed outputs, stored and visualized efficiently.

## Batch Layer

The batch layer processes historical flight data using the following scripts:

- **`spark-preprocessing.py`**: Cleans and transforms raw CSV data into a structured format.
- **`spark.py`**: Performs batch analytics (e.g., aggregations, summaries).
- **`spark-ml.py`**: Applies machine learning models (e.g., delay prediction).

Run these scripts inside the Spark container (see "How to Run" section).

## Speed Layer

The speed layer processes real-time flight data:

- **`producer.py`**: Simulates real-time flight data and sends it to Kafka. Run it on the host machine:
  ```bash
  python producer.py
  ```
- **`spark-streaming.py`**: Consumes Kafka streams and processes them with Spark Streaming. Run it inside the Spark container.

## Serving Layer (Visualization)

Processed data is stored in Elasticsearch and visualized with Kibana:

- **Elasticsearch**: Stores batch and real-time results.
- **Kibana**: Access at `http://localhost:5601`. Create index patterns (e.g., `flight-data-*`) and build dashboards.

## How to Run

1. **Start the environment**:
   ```bash
   docker-compose up -d
   ```
   This launches Kafka, Elasticsearch, Kibana, and Spark containers.

2. **Upload data to HDFS** (if not done yet):
   ```bash
   hdfs dfs -put ./data/*.csv /flight-data
   ```

3. **Run batch layer scripts**:
   Access the Spark container:
   ```bash
   docker exec -it spark-master bash
   ```
   Then run:
   ```bash
   spark-submit /path/to/spark-preprocessing.py
   spark-submit /path/to/spark.py
   spark-submit /path/to/spark-ml.py
   ```

4. **Run speed layer**:
   - On the host: `python producer.py`
   - In the Spark container: `spark-submit /path/to/spark-streaming.py`

5. **Visualize results**:
   Open Kibana at `http://localhost:5601`, create an index pattern, and build dashboards.

**Note**: Check container logs with `docker logs <container_name>` if errors occur.

## Results

After running the project, expect:

- **Batch Layer**: Historical insights (e.g., average delays by airline).
- **Speed Layer**: Real-time metrics (e.g., live flight status).
- **Visualization**: Dashboards in Kibana showing trends and predictions.

Sample dashboard: A line chart of flight delays over time or a heatmap of delay hotspots.