import argparse
import json
import requests
import logging
from pyspark.sql import SparkSession

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_from_api(url: str, save_path: str, file_name: str):
    spark = None
    try:
        spark = SparkSession.builder.appName("Extact_from_API").getOrCreate()
        logging.info("Spark session created successfully.")

        response = requests.get(url)

        # ตรวจสอบว่าการเรียก API สําเร็จหรือไม่ ถ้าไม่สําเร็จให้จะ throw error
        response.raise_for_status()
        
        # แปลงจาก json เป็น object ถ้าไม่มี error
        data = response.json()

        # แปลงเป็น Spark DataFrame
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
            
        # Path to the JSON file
        save_path = f"{save_path}/{file_name}"
            
        # Save โดยตรงไป MinIO/S3
        df.write.mode("overwrite").json(save_path)
        logging.info(f"Data saved to MinIO/S3: {save_path}")

    except requests.exceptions.RequestException as e:
        # จัดการข้อผิดพลาดในการเรียก API
        logging.error(f"An error occurred while calling the API: {str(e)}")

    except json.JSONDecodeError as e:
        # จัดการข้อผิดพลาดในการแปลง JSON เป็น object
        logging.error(f"An error occurred while decoding JSON: {str(e)}")

    except Exception as e:
        # จัดการข้อผิดพลาดอื่น ๆ ที่ไม่คาดคิด
        logging.critical(f"An error occurred: {str(e)}")
    
    finally:
        if spark is not None:
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--save_path", required=True)
    parser.add_argument("--file_name", required=True)
    args = parser.parse_args()

    extract_from_api(args.url, args.save_path, args.file_name)
