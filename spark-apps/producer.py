#!/usr/bin/env python3

from kafka import KafkaProducer
import json
import csv
import time
import random

# --- Cấu hình Kafka ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC = 'live-data'

DATA_SOURCE_FILE = '/home/minh/codeproject/bigdata/data/2024_3.csv'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k else None
)

print(f"Kafka Producer đang kết nối tới: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Sẽ gửi dữ liệu vào topic: {KAFKA_TOPIC}")
print(f"Đọc dữ liệu từ file: {DATA_SOURCE_FILE}")

def send_flight_data():
    try:
        with open(DATA_SOURCE_FILE, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            message_count = 0
            for row_index, row in enumerate(reader):
                try:
                    flight_event = {
                        "FL_DATE": row.get("FlightDate"), 
                        "OP_CARRIER": row.get("IATA_CODE_Reporting_Airline"),
                        "OP_CARRIER_FL_NUM": row.get("Flight_Number_Reporting_Airline"),
                        "ORIGIN": row.get("Origin"),
                        "DEST": row.get("Dest"),
                        "DEP_DELAY": float(row.get("DepDelay", 0.0)) if row.get("DepDelay") else 0.0,
                        "ARR_DELAY": float(row.get("ArrDelay", 0.0)) if row.get("ArrDelay") else 0.0,
                        "CANCELLED": int(float(row.get("Cancelled", 0))) if row.get("Cancelled") else 0,
                        "DIVERTED": int(float(row.get("Diverted", 0))) if row.get("Diverted") else 0,   
                        "DISTANCE": float(row.get("Distance", 0.0)) if row.get("Distance") else 0.0,
                        "TAXI_OUT": float(row.get("TaxiOut", 0.0)) if row.get("TaxiOut") else 0.0,
                        "TAXI_IN": float(row.get("TaxiIn", 0.0)) if row.get("TaxiIn") else 0.0,
                        "AIR_TIME": float(row.get("AirTime", 0.0)) if row.get("AirTime") else 0.0
                    }
                    message_key = flight_event.get("FL_DATE") # Ví dụ

                    producer.send(topic=KAFKA_TOPIC, key=message_key, value=flight_event)
                    message_count += 1

                    if message_count % 10 == 0:
                        print(f"Đã gửi {message_count} tin nhắn. Tin nhắn cuối: {flight_event.get('OP_CARRIER')}-{flight_event.get('OP_CARRIER_FL_NUM')} vào {flight_event.get('FL_DATE')}")
                        producer.flush() 
                        time.sleep(random.uniform(0.1, 0.5))

                    if message_count % 100 == 0:
                         time.sleep(random.uniform(0.5, 1))


                except Exception as e_row:
                    print(f"Lỗi khi xử lý dòng {row_index + 1}: {row}. Lỗi: {e_row}")
                    continue

            producer.flush() 
            print(f"Hoàn tất gửi {message_count} tin nhắn.")

    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file dữ liệu nguồn: {DATA_SOURCE_FILE}")
    except Exception as e:
        print(f"Lỗi không mong muốn trong producer: {e}")
    finally:
        if producer:
            producer.close()
            print("Đã đóng Kafka Producer.")

if __name__ == "__main__":
    send_flight_data()