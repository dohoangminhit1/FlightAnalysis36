#!/usr/bin/env python3
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, month, avg, year, dayofweek, count, when, sum, last_day, next_day, dayofyear, \
    dayofmonth, datediff, row_number, lit, max

# --- Cấu hình ---
HDFS_NAMENODE = "hdfs://host.docker.internal:9000" # Hoặc "hdfs://192.168.1.155:9000"
PROCESSED_DATA_PATH_PARQUET = f"{HDFS_NAMENODE}/bigdata/processed_data/historical_flights.parquet"

ELASTICSEARCH_NODES = "http://elasticsearch:9200"
ES_INDEX_PREFIX = "flight_batch_analysis_" # Prefix cho các index trong Elasticsearch

def write_to_elasticsearch(df, index_name, id_column=None):
    """Hàm tiện ích để ghi DataFrame vào Elasticsearch."""
    writer = df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", f"{ES_INDEX_PREFIX}{index_name}") \
        .option("es.nodes", ELASTICSEARCH_NODES) \
        .mode("overwrite") # Hoặc "append" tùy nhu cầu
    if id_column:
        writer = writer.option("es.mapping.id", id_column)
    writer.save()
    print(f"Đã ghi dữ liệu vào Elasticsearch index: {ES_INDEX_PREFIX}{index_name}")

def main():
    spark = SparkSession \
        .builder \
        .appName("Flight Batch Analysis (to Elasticsearch - Simplified)") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-35_2.12:8.14.1")\
        .config("spark.elasticsearch.nodes", ELASTICSEARCH_NODES) \
        .config("spark.elasticsearch.port", "9200") \
        .config("spark.elasticsearch.nodes.wan.only", "false") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()

    print(f"Đang đọc dữ liệu đã tiền xử lý từ Parquet: {PROCESSED_DATA_PATH_PARQUET}")
    df = spark.read.parquet(PROCESSED_DATA_PATH_PARQUET) # Không dùng .cache() để nhất quán với spark-ml.py
    df.printSchema()
    print(f"Số dòng đọc được từ Parquet: {df.count()}")

    """
    ========================================================================================
    Delay Analysis per Carrier
    ========================================================================================
    """
    print("\n--- Phân tích Độ trễ theo Hãng hàng không ---")
    delay_total_df = df.select("OP_CARRIER", "DEP_DELAY", "ARR_DELAY") \
        .groupBy("OP_CARRIER") \
        .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY"))
    write_to_elasticsearch(delay_total_df, "s_delay_total_carrier", id_column="OP_CARRIER")

    delay_year_df = df.select("OP_CARRIER", "YEAR", "DEP_DELAY", "ARR_DELAY") \
        .groupBy("OP_CARRIER", "YEAR") \
        .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY"))
    write_to_elasticsearch(delay_year_df, "s_delay_year_carrier")

    delay_year_month_df = df.select("OP_CARRIER", "YEAR", "MONTH", "DEP_DELAY", "ARR_DELAY") \
        .groupBy("OP_CARRIER", "YEAR", "MONTH") \
        .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY"))
    write_to_elasticsearch(delay_year_month_df, "s_delay_year_month_carrier")

    delay_dayofweek_df = df.select("OP_CARRIER", "DAYOFWEEK", "DEP_DELAY", "ARR_DELAY") \
        .groupBy("OP_CARRIER", "DAYOFWEEK") \
        .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY"))
    write_to_elasticsearch(delay_dayofweek_df, "s_delay_dayofweek_carrier")

    """
    ========================================================================================
    Cancellation & Diverted Analysis per Carrier
    ========================================================================================
    """
    print("\n--- Phân tích Hủy chuyến & Chuyển hướng theo Hãng hàng không ---")
    cancellation_diverted_total_df = df.select("OP_CARRIER", "CANCELLED", "DIVERTED") \
        .groupBy("OP_CARRIER") \
        .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("TOTAL_FLIGHTS")) \
        .withColumn("DIV_PERC", (col("DIV_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .withColumn("CANC_PERC", (col("CANC_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .select("OP_CARRIER", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT", "TOTAL_FLIGHTS") \
        .orderBy("OP_CARRIER")
    write_to_elasticsearch(cancellation_diverted_total_df, "s_canc_div_total_carrier", id_column="OP_CARRIER")

    cancellation_diverted_year_df = df.select("OP_CARRIER", "YEAR", "CANCELLED", "DIVERTED") \
        .groupBy("OP_CARRIER", "YEAR") \
        .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("TOTAL_FLIGHTS")) \
        .withColumn("DIV_PERC", (col("DIV_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .withColumn("CANC_PERC", (col("CANC_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .select("OP_CARRIER", "YEAR", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT", "TOTAL_FLIGHTS") \
        .orderBy("YEAR", "OP_CARRIER")
    write_to_elasticsearch(cancellation_diverted_year_df, "s_canc_div_year_carrier")

    cancellation_diverted_year_month_df = df.select("OP_CARRIER", "YEAR", "MONTH", "CANCELLED", "DIVERTED") \
        .groupBy("OP_CARRIER", "YEAR", "MONTH") \
        .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("TOTAL_FLIGHTS")) \
        .withColumn("DIV_PERC", (col("DIV_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .withColumn("CANC_PERC", (col("CANC_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .select("OP_CARRIER", "YEAR", "MONTH", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT", "TOTAL_FLIGHTS") \
        .orderBy("YEAR", "MONTH", "OP_CARRIER")
    write_to_elasticsearch(cancellation_diverted_year_month_df, "s_canc_div_year_month_carrier")

    cancellation_diverted_dayofweek_df = df.select("OP_CARRIER", "DAYOFWEEK", "CANCELLED", "DIVERTED") \
        .groupBy("OP_CARRIER", "DAYOFWEEK") \
        .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("TOTAL_FLIGHTS")) \
        .withColumn("DIV_PERC", (col("DIV_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .withColumn("CANC_PERC", (col("CANC_COUNT") / col("TOTAL_FLIGHTS") * 100.0)) \
        .select("OP_CARRIER", "DAYOFWEEK", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT", "TOTAL_FLIGHTS") \
        .orderBy("DAYOFWEEK", "OP_CARRIER")
    write_to_elasticsearch(cancellation_diverted_dayofweek_df, "s_canc_div_dayofweek_carrier")

    """
    ========================================================================================
    Distance Analysis per Carrier
    ========================================================================================
    """
    print("\n--- Phân tích Khoảng cách theo Hãng hàng không ---")
    dist_total_df = df.select("OP_CARRIER", "DISTANCE") \
        .filter(col("DISTANCE").isNotNull()) \
        .groupBy("OP_CARRIER") \
        .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
        .orderBy("OP_CARRIER")
    write_to_elasticsearch(dist_total_df, "s_dist_total_carrier", id_column="OP_CARRIER")

    dist_year_df = df.select("OP_CARRIER", "YEAR", "DISTANCE") \
        .filter(col("DISTANCE").isNotNull()) \
        .groupBy("OP_CARRIER", "YEAR") \
        .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
        .orderBy("YEAR", "OP_CARRIER")
    write_to_elasticsearch(dist_year_df, "s_dist_year_carrier")

    dist_year_month_df = df.select("OP_CARRIER", "YEAR", "MONTH", "DISTANCE") \
        .filter(col("DISTANCE").isNotNull()) \
        .groupBy("OP_CARRIER", "YEAR", "MONTH") \
        .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
        .orderBy("YEAR", "MONTH", "OP_CARRIER")
    write_to_elasticsearch(dist_year_month_df, "s_dist_year_month_carrier")

    dist_dayofweek_df = df.select("OP_CARRIER", "DAYOFWEEK", "DISTANCE") \
        .filter(col("DISTANCE").isNotNull()) \
        .groupBy("OP_CARRIER", "DAYOFWEEK") \
        .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
        .orderBy("DAYOFWEEK", "OP_CARRIER")
    write_to_elasticsearch(dist_dayofweek_df, "s_dist_dayofweek_carrier")

    """
    ========================================================================================
    Max consec days of Delay Analysis per Carrier
    ========================================================================================
    """
    print("\n--- Phân tích Số ngày trễ liên tiếp tối đa theo Hãng hàng không ---")
    df_for_consec = df.filter(col("FL_DATE").isNotNull() & col("ARR_DELAY").isNotNull())

    max_consec_delay_year_df = df_for_consec.select("OP_CARRIER", "FL_DATE", "YEAR", "ARR_DELAY") \
        .groupBy("OP_CARRIER", "YEAR", "FL_DATE") \
        .agg(avg(col("ARR_DELAY")).alias("AVG_DAILY_ARR_DELAY")) \
        .filter(col("AVG_DAILY_ARR_DELAY") > 0) \
        .withColumn("ROW_NUMBER", row_number().over(Window.partitionBy("OP_CARRIER", "YEAR").orderBy("OP_CARRIER", "YEAR", "FL_DATE"))) \
        .withColumn("GRP", datediff(col("FL_DATE"), lit("1900-01-01")) - col("ROW_NUMBER")) \
        .withColumn("DAYS_IN_CONSEC_DELAY", row_number().over(Window.partitionBy("OP_CARRIER", "YEAR", "GRP").orderBy("OP_CARRIER", "YEAR", "FL_DATE"))) \
        .groupBy("OP_CARRIER", "YEAR") \
        .agg(max(col("DAYS_IN_CONSEC_DELAY")).alias("MAX_CONSEC_DELAY_DAYS"))
    write_to_elasticsearch(max_consec_delay_year_df, "s_max_consec_delay_year_carrier")

    print("\n--- Các phân tích theo Nguồn-Đích đã được loại bỏ để tinh giản ---")

    print("\nHoàn tất tất cả các phân tích (phiên bản tinh giản) và ghi vào Elasticsearch.")
    spark.stop()

if __name__ == "__main__":
    main()