#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, dayofweek, avg, sum, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType, DateType # Thêm DateType
from pyspark.ml.pipeline import PipelineModel
import json

# --- Cấu hình ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" # Chạy từ trong container
KAFKA_TOPIC = "live-data"

HDFS_NAMENODE = "hdfs://host.docker.internal:9000"
DELAY_PIPELINE_OUTPUT_PATH = f"{HDFS_NAMENODE}/bigdata/models/delay_prediction_pipeline"
CANCELLATION_PIPELINE_OUTPUT_PATH = f"{HDFS_NAMENODE}/bigdata/models/cancellation_prediction_pipeline" # Nếu muốn dùng cả model hủy chuyến

ELASTICSEARCH_NODES = "http://elasticsearch:9200"
ES_INDEX_PREFIX = "flight_streaming_"

def main():
    spark = SparkSession \
        .builder \
        .appName("Flight Streaming Analysis with ML and ES") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.14.1") \
        .config("spark.elasticsearch.nodes", ELASTICSEARCH_NODES) \
        .config("spark.elasticsearch.port", "9200") \
        .config("spark.elasticsearch.nodes.wan.only", "false") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN") # Giảm bớt log

    print(f"Đang tải mô hình dự đoán độ trễ từ: {DELAY_PIPELINE_OUTPUT_PATH}")
    try:
        delay_model = PipelineModel.load(DELAY_PIPELINE_OUTPUT_PATH)
        print("Tải mô hình dự đoán độ trễ thành công.")
    except Exception as e:
        print(f"LỖI khi tải mô hình dự đoán độ trễ: {e}")
        print("Kiểm tra đường dẫn và đảm bảo mô hình đã được huấn luyện và lưu đúng cách.")
        delay_model = None # Sẽ không thực hiện dự đoán nếu không tải được model

    # --- Định nghĩa Schema cho dữ liệu JSON từ Kafka ---
    # Dựa trên output của producer.py (phiên bản đã sửa)
    flight_schema = StructType([
        StructField("FL_DATE", StringType(), True), # YYYY-MM-DD
        StructField("OP_CARRIER", StringType(), True),
        StructField("OP_CARRIER_FL_NUM", StringType(), True), # Giữ là String, nếu cần int thì cast sau
        StructField("ORIGIN", StringType(), True),
        StructField("DEST", StringType(), True),
        StructField("DEP_DELAY", FloatType(), True),
        StructField("ARR_DELAY", FloatType(), True),
        StructField("CANCELLED", IntegerType(), True), # 0 or 1
        StructField("DIVERTED", IntegerType(), True),   # 0 or 1
        StructField("DISTANCE", FloatType(), True),
        StructField("TAXI_OUT", FloatType(), True),
        StructField("TAXI_IN", FloatType(), True),
        StructField("AIR_TIME", FloatType(), True),
        # Thêm các trường khác nếu producer gửi và bạn cần
        # Ví dụ: "CRS_DEP_TIME", "DEP_TIME" nếu producer không tính mà bạn muốn tính ở đây
    ])

    # --- Đọc Stream từ Kafka ---
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # --- Parse JSON và chọn cột ---
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), flight_schema).alias("data")
    ).select("data.*")

    # --- Tạo các cột dẫn xuất thời gian và cột IS_DELAY thực tế ---
    # Chuyển FL_DATE từ String sang DateType để dùng các hàm year, month,...
    df_transformed = df_parsed \
        .withColumn("FL_DATE_TS", col("FL_DATE").cast(DateType())) \
        .withColumn("YEAR", year(col("FL_DATE_TS"))) \
        .withColumn("MONTH", month(col("FL_DATE_TS"))) \
        .withColumn("DAYOFMONTH", dayofmonth(col("FL_DATE_TS"))) \
        .withColumn("DAYOFWEEK", dayofweek(col("FL_DATE_TS"))) \
        .withColumn("IS_DELAY_ACTUAL", (col("ARR_DELAY") > 0).cast("int")) # Cột này là ground truth

    # --- Áp dụng Mô hình Dự đoán Độ trễ (nếu tải thành công) ---
    df_with_predictions = df_transformed # Khởi tạo

    if delay_model:
        print("Chuẩn bị dữ liệu và áp dụng mô hình dự đoán độ trễ...")
        # Các cột features mà mô hình delay cần: "MONTH", "DAYOFMONTH", "DAYOFWEEK", "OP_CARRIER", "DISTANCE"
        # Model Pipeline (StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, LinearSVC) sẽ tự xử lý.
        # Chúng ta chỉ cần đảm bảo các cột đầu vào cho pipeline có mặt.
        # Các cột 'INDEX_CARRIER', 'ONEHOT_CARRIER', 'FEATURES_UNSCALED', 'FEATURES' sẽ được model tự tạo.
        
        # Chọn các cột cần thiết cho model và giữ lại các cột khác để ghi ra ES
        # Đảm bảo các cột này tồn tại trong df_transformed
        cols_for_model = ["MONTH", "DAYOFMONTH", "DAYOFWEEK", "OP_CARRIER", "DISTANCE"]
        missing_cols = [c for c in cols_for_model if c not in df_transformed.columns]
        if not missing_cols:
            predictions_df = delay_model.transform(df_transformed) # Pass all existing columns and ensure model features are present
            df_with_predictions = predictions_df.withColumnRenamed("prediction", "PREDICTED_IS_DELAY") \
                                            .drop("INDEX_CARRIER", "ONEHOT_CARRIER", "FEATURES_UNSCALED", "FEATURES", "rawPrediction") # Dọn dẹp cột trung gian của model
            print("Đã thêm cột PREDICTED_IS_DELAY.")
        else:
            print(f"LỖI: Thiếu các cột cần thiết cho mô hình: {missing_cols}. Sẽ không thực hiện dự đoán.")
            df_with_predictions = df_transformed.withColumn("PREDICTED_IS_DELAY", lit(None).cast(IntegerType()))
    else:
        df_with_predictions = df_transformed.withColumn("PREDICTED_IS_DELAY", lit(None).cast(IntegerType()))


    # --- Ghi dữ liệu thô (đã parse, transform và có dự đoán) vào Elasticsearch ---
    def write_raw_to_es(batch_df, epoch_id):
        # ... (logic ghi dữ liệu thô) ...
        index_name_suffix = "raw_flights_with_predictions"
        print(f"Epoch {epoch_id}: Ghi {batch_df.count()} dòng vào {ES_INDEX_PREFIX}{index_name_suffix}")
        try:
            batch_df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", f"{ES_INDEX_PREFIX}{index_name_suffix}") \
                .option("es.nodes", ELASTICSEARCH_NODES) \
                .option("es.spark.dataframe.write.null", "true") \
                .mode("append") \
                .save() # Chỗ này là save() của batch write trong foreachBatch, không phải writeStream
            print(f"Ghi vào {ES_INDEX_PREFIX}{index_name_suffix} thành công.")
        except Exception as e_es_write:
            print(f"LỖI khi ghi vào Elasticsearch (raw): {e_es_write}")


    query_raw_to_es = df_with_predictions.writeStream \
        .outputMode("append") \
        .foreachBatch(write_raw_to_es) \
        .trigger(processingTime="30 seconds") \
        .start()

    # --- Phân tích Độ trễ Trung bình theo Hãng và Tháng (Tương tự `spark.py`) ---
    delay_agg_df = df_transformed \
        .groupBy(col("OP_CARRIER"), col("YEAR"), col("MONTH")) \
        .agg(
            avg("DEP_DELAY").alias("AVG_DEP_DELAY"),
            avg("ARR_DELAY").alias("AVG_ARR_DELAY"),
            sum(col("CANCELLED")).alias("TOTAL_CANCELLED"),
            sum(col("DIVERTED")).alias("TOTAL_DIVERTED"),
            sum(col("DISTANCE")).alias("TOTAL_DISTANCE")
        )

    def write_agg_to_es(batch_df, epoch_id):
        # ... (logic ghi dữ liệu tổng hợp) ...
        index_name_suffix = "agg_carrier_year_month_stats"
        print(f"Epoch {epoch_id}: Ghi {batch_df.count()} dòng tổng hợp vào {ES_INDEX_PREFIX}{index_name_suffix}")
        try:
            # Chú ý: khi dùng foreachBatch, batch_df là một static DataFrame
            # Chúng ta đang ghi nó vào ES, không phải là config output mode cho writeStream ở đây.
            # Output mode của writeStream đã được đặt ở dưới.
            batch_df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", f"{ES_INDEX_PREFIX}{index_name_suffix}") \
                .option("es.nodes", ELASTICSEARCH_NODES) \
                .option("es.spark.dataframe.write.null", "true") \
                .mode("overwrite") \
                .save()
            print(f"Ghi vào {ES_INDEX_PREFIX}{index_name_suffix} thành công.")
        except Exception as e_es_agg_write:
            print(f"LỖI khi ghi vào Elasticsearch (agg): {e_es_agg_write}")

    query_agg_to_es = delay_agg_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_agg_to_es) \
        .trigger(processingTime="60 seconds") \
        .start()

    print("Các streaming queries đã bắt đầu...")
    # spark.streams.awaitAnyTermination() # Comment dòng này để xem lỗi cụ thể từ query nào nếu có

    # Thay vì awaitAnyTermination, hãy thử await từng query để debug
    try:
        query_raw_to_es.awaitTermination()
    except Exception as e_raw:
        print(f"Lỗi với query_raw_to_es: {e_raw}")

    try:
        query_agg_to_es.awaitTermination()
    except Exception as e_agg:
        print(f"Lỗi với query_agg_to_es: {e_agg}")

    # Nếu tất cả chạy ổn, bạn có thể quay lại dùng:
    # spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()