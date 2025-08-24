import argparse
from pyspark.sql import SparkSession, DataFrame
from plugins.spark_helpers.utils.data_clean_utils import handle_missing_values
import logging

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def handle_missing(file_name: str, read_path: str, save_path: str) -> DataFrame:
    spark = None
    try:
        # สร้าง SparkSession
        spark = SparkSession.builder.getOrCreate()
        logging.info("Spark session created successfully.")

        # อ่าน join table เป็น Parquet
        df = spark.read.parquet(f"{read_path}/{file_name}")

        # จัดการ missing values
        df = handle_missing_values(df)
        logging.info("Missing values handled successfully.")
        
        # แสดง Schema
        df.printSchema()

        # save ลงไฟล์
        df.write.mode("overwrite").parquet(f"{save_path}/handle_missing")
        logging.info("Data saved to handle_missing successfully.")    

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    
    finally:
        if spark is not None:
            # ปิด SparkSession
            spark.stop()
            logging.info("Spark session closed successfully.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_name", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    parser.add_argument("--read_path", type=str, required=True)
    args = parser.parse_args()
    
    handle_missing(args.file_name, args.read_path, args.save_path)