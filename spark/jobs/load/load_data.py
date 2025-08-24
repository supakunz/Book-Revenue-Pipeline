import logging
import argparse
from pyspark.sql import SparkSession
from plugins.spark_helpers.utils.filename_to_date import get_filename_with_date 

# กำหนดค่า logging เพื่อให้แสดงผลทันที
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(read_path: str, save_path: str):
    spark = None
    try:
        spark = SparkSession.builder.getOrCreate()
        logging.info("Spark session created successfully.")

        # อ่านไฟล์ Parquet
        df = spark.read.parquet(f"{read_path}/transform")
        
        # แสดง Schema
        df.printSchema()

        file_path = get_filename_with_date(save_path, 'output')
        
        # รวมเป็น 1 task
        # df = df.repartition(1)

        df.write.mode("overwrite").csv(file_path)
        logging.info("Data loaded successfully.")
        
    except Exception as e:
        # จัดการข้อผิดพลาดอื่น ๆ ที่ไม่คาดคิด
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_path", type=str, required=True)
    parser.add_argument("--read_path", type=str, required=True)
    args = parser.parse_args()
    
    load_data(args.read_path, args.save_path)
