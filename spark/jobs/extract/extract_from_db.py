import argparse
from pyspark.sql import DataFrame, SparkSession
from plugins.spark_helpers.utils.filename_to_date import get_filename_with_date 
import logging

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_from_db(jdbc_url:str, user:str, password:str ,table:str, save_path:str, file_name:str) -> DataFrame:
    spark = None # ตัวแปรสําหรับ SparkSession
    try:
      spark = SparkSession.builder.appName("Extact_from_DB").getOrCreate()
      logging.info("Spark session created successfully.")

      # สร้างคําสั่ง SQL
      sql_query = f"(SELECT * FROM {table} LIMIT 1000) AS subquery_data" # ต้องมี AS ตามด้วยชื่อ alias ที่คุณตั้งขึ้นมา Spark จะได้ไม่เกิดข้อผิดพลาด 

      # อ่านข้อมูลจาก database โดยใช้ JDBC connection
      df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", sql_query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
      logging.info("Data loaded from the database successfully.")

      # แสดง Schema ของ DataFrame
      df.printSchema()
      
      # แปลงชื่อไฟล์ให้มีวันที่
      filename = get_filename_with_date(save_path, file_name)

      # บันทึกลงไฟล์แบบ Parquet
      df.write.mode("overwrite").parquet(filename)
      logging.info(f"Data saved to {filename} successfully.")

      return df 

    except Exception as e:
      # จัดการข้อผิดพลาดทั้งหมดที่อาจเกิดชขึ้น
      logging.error(f"An error occurred while extracting data from DB: {str(e)}")

    finally:
      # ส่วนนี้จะทำงานเสมอ ไม่ว่าจะเกิด error หรือไม่
      if spark is not None:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc_url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--save_path", required=True)
    parser.add_argument("--file_name", required=True)
    args = parser.parse_args()

    extract_from_db(args.jdbc_url, args.user, args.password, args.table, args.save_path, args.file_name)