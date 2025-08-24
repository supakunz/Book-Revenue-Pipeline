import argparse
from pyspark.sql import SparkSession
import logging

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def join_table(db_file_name: str ,db_path: str, api_file_name: str, api_path: str, save_path: str):
    spark = None
    try:
        # สร้าง SparkSession
        spark = SparkSession.builder.getOrCreate()
        logging.info("Spark session created successfully.")

        # อ่านข้อมูล DB เป็น Parquet
        df_db = spark.read.parquet(f"{db_path}/{db_file_name}_cleaned")
        # อ่านข้อมูล API เป็น Parquet
        df_api = spark.read.parquet(f"{api_path}/{api_file_name}_cleaned")
    
        # Join ข้อมูล DB และ API ด้วย date
        joined_df = df_db.join(df_api, on="date", how="left")
        logging.info("Joined table successfully.")

        # แสดง Schema
        joined_df.printSchema()

        # ตั้งชื่อไฟล์
        filename = f"{save_path}/join_table"
        
        # # แบ่งงานหลาย task ลด memory ต่อ executor
        df = joined_df.repartition(16)
        
        # # บันทึกลงไฟล์แบบ Parquet
        df.write.mode("overwrite").parquet(filename)
        logging.info(f"Data saved to {filename} successfully.")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise # ส่ง Exception ออกไปเพื่อให้ Airflow สามารถจับและแสดงสถานะ failed

    finally:
        if spark is not None:
            spark.stop()
            logging.info("Spark session stopped successfully.")
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_file_name", type=str, required=True)
    parser.add_argument("--db_path", type=str, required=True)
    parser.add_argument("--api_file_name", type=str, required=True)
    parser.add_argument("--api_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    args = parser.parse_args()
    
    join_table(args.db_file_name, args.db_path, args.api_file_name, args.api_path, args.save_path)