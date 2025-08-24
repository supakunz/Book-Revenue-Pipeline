import argparse
from pyspark.sql import SparkSession, DataFrame
from dags.spark_jobs.transform.data_clean_utils import remove_invalid_rows
from dags.spark_jobs.transform.eda_utils import remove_outliers
import logging

# ตั้งค่า logging เพื่อบันทึกข้อผิดพลาด
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def handle_missing_final(read_path: str, save_path: str) -> DataFrame:
    spark = None
    try:
        # สร้าง SparkSession
        spark = SparkSession.builder.getOrCreate()
        logging.info("Spark session created successfully.")

        df = spark.read.parquet(f"{read_path}/handle_missing")

        df = remove_invalid_rows(df)
        df = remove_outliers(df)
        
        df.printSchema()

        # แบ่งงานเป็น 16 task
        df = df.repartition(16)

        # save ลงไฟล์
        df.write.mode("overwrite").parquet(f"{save_path}/handle_missing_final")
        logging.info("Data saved to handle_missing_final successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logging.info("Spark session stopped.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_path", type=str, required=True)
    parser.add_argument("--read_path", type=str, required=True)
    args = parser.parse_args()
    
    handle_missing_final(args.read_path, args.save_path)