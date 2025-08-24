from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import round, col
import logging
import argparse

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform(read_path: str, save_path: str):
    spark = None
    try:
        # สร้าง SparkSession
        spark = SparkSession.builder.getOrCreate()
        logging.info("Spark session created successfully.")

        # อ่านไฟล์ Parquet
        df = spark.read.parquet(f"{read_path}/handle_missing_final")
        
        # สร้าง column THB และ ลบ column ไม่ต้องการ
        df = df.withColumn( "thb_price", round(col("price") * col("conversion_rate"), 2))
        df = df.drop("date","id")

        # แสดง Schema
        df.printSchema()
    
        # แบ่งงานเป็น 16 task
        df = df.repartition(16)

        # save ลงไฟล์
        df.write.mode("overwrite").parquet(f"{save_path}/transform")
        logging.info("Data saved to transform successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    finally:
        if spark is not None:
            spark.stop()
            logging.info("Spark session stopped.")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_path", type=str, required=True)
    parser.add_argument("--read_path", type=str, required=True)
    args = parser.parse_args()
    
    transform(args.read_path, args.save_path)