import argparse
from pyspark.sql import SparkSession
from plugins.spark_helpers.utils.data_clean_utils import normalize_column_names, cast_column_types, handle_missing_values, remove_invalid_rows, re_price_name
from plugins.spark_helpers.utils.read_parquet import read_latest_parquet
import logging

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_data(source: str, read_path: str, save_path: str, file_name: str):
    spark = None
    try:
        # สร้าง SparkSession
        spark = SparkSession.builder.getOrCreate()
        logging.info("Spark session created successfully.")
        
        if source == "data_from_db":
            # อ่านข้อมูล DB เป็น Parquet
            df = read_latest_parquet(spark, read_path, file_name)
            logging.info("Data loaded from the database successfully.")
        elif source == "data_from_api":
            # อ่านข้อมูล API เป็น Parquet
            df = spark.read.json(f"{read_path}/{file_name}")
            logging.info("Data loaded from the API successfully.")
        
        # ทําการ clean ข้อมูลเบื้องต้น
        df = normalize_column_names(df)
        df = cast_column_types(source, df)
        logging.info("Data cleaned successfully.")

        # แสดง Schema หลัง clean
        df.printSchema()

        # ตั้งชื่อไฟล์
        filename = f"{save_path}/{source}_cleaned"
        
        # บันทึกลงไฟล์แบบ Parquet
        df.write.mode("overwrite").parquet(filename)
        logging.info(f"Data saved to {filename} successfully.")    
        
    except Exception as e:
        # จัดการข้อผิดพลาด
        logging.error(f"An error occurred: {str(e)}")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--read_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    parser.add_argument("--file_name", type=str, required=True)
    args = parser.parse_args()
    
    clean_data(args.source, args.read_path, args.save_path, args.file_name)