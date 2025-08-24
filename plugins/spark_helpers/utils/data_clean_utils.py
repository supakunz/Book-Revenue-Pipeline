from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import coalesce, expr, col, lower, to_date, lit, trim, to_timestamp, when, regexp_replace
from functools import reduce
from pyspark.sql.functions import regexp_replace, trim, col


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    เปลี่ยนชื่อ column ให้เป็น lowercase + snake_case
    """
    for c in df.columns:
        new_name = c.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(c, new_name)
    return df


def cast_column_types(source: str,df: DataFrame) -> DataFrame:
    """
    กำหนด dtype ที่ถูกต้องตาม schema ที่ใช้ downstream
    """
    if source == "data_from_db":
      return (
            df.withColumn("timestamp", to_timestamp("timestamp", "yyyy/MM/dd HH:mm:ss"))
            .withColumnRenamed("total_no._of_rating", "total_rating_count")
            .withColumn("date", to_date(col("timestamp")))
      )
    
    elif source == "data_from_api":
      return (df.withColumn("date", to_date(col("date")))
              .withColumn("conversion_rate", expr("try_cast(conversion_rate as double)")))
    
    else:
      return ValueError(f"Unknown data source: {source}")

    
def handle_missing_values(df: DataFrame) -> DataFrame:
    # 1. จัดการ fake nulls และค่าว่างในทุกคอลัมน์ก่อนการกรอง
    columns_to_check = [
        "date", "timestamp", "user_id", "book_id", "country",
        "book_title", "book_subtitle", "book_author", "book_narrator",
        "audio_runtime", "audiobook_type", "categories",
        "rating", "total_rating_count", "price", "conversion_rate", "id"
    ]

    # Regular expression pattern for UUID
    uuid_pattern = r'^[0-9a-fA-F]{8}$'

    existing_columns = [c for c in columns_to_check if c in df.columns]

    # จัดการ fake nulls
    for c in existing_columns:
        df = df.withColumn(c, 
            when(
                (trim(lower(col(c))) == "null") |
                (trim(col(c)) == ""), 
                lit(None) # แทนที่ด้วย null
            ).otherwise(trim(col(c)))
        )
    
    # 2. จัดการ user_id ตาม uuid_pattern
    df = df.withColumn(
        "user_id",
        when(col("user_id").rlike(uuid_pattern), col("user_id")).otherwise(None)
    )

    # 3. ตอนนี้ข้อมูลที่เคยเป็น fake nulls ถูกเปลี่ยนเป็น null แล้ว
    df = df.na.drop(subset=existing_columns)

    # 4. จัดการ subtitle เมื่อมี null ที่เหลือด้วย No Subtitle
    if "book_subtitle" in df.columns:
        df = df.withColumn(
            "book_subtitle",
            coalesce(col("book_subtitle"), lit("No Subtitle"))
        )

    # 5. Clean data Type
    # ตรวจสอบว่าคอลัมน์มีอยู่จริงก่อนจะดำเนินการ
    if "price" in df.columns:
        df = df.withColumn("price", regexp_replace(trim(col("price")), "[$\r]", "").cast("float"))

    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        # สร้างคอลัมน์ date จาก timestamp ที่แปลงแล้ว
        df = df.withColumn("date", to_date(col("timestamp")))

    if "book_id" in df.columns:
        df = df.withColumn("book_id", expr("try_cast(book_id as int)"))

    if "rating" in df.columns:
        df = df.withColumn("rating", expr("try_cast(rating as double)"))

    if "total_rating_count" in df.columns:
        df = df.withColumn("total_rating_count", expr("try_cast(total_rating_count as int)"))

    if "conversion_rate" in df.columns:
        df = df.withColumn("conversion_rate", regexp_replace(trim(col("conversion_rate")), "[$\r]", "").cast("float"))

    return df


def remove_invalid_rows(df: DataFrame) -> DataFrame:
    """
    กรองแถวที่มีค่าไม่สมเหตุผล เช่น ราคาติดลบ ปริมาณติดลบ
    """
    return df.filter((col("book_id") >= 0) & 
                       (col("rating") >= 0) & 
                       (col("total_rating_count") >= 0) & 
                       (col("price") >= 0) &
                       (col("conversion_rate") >= 0))


def standardize_text(df: DataFrame) -> DataFrame:
    """
    ทำให้ค่าข้อความ standard เช่น lower case, ตัดช่องว่าง
    """
    return (
        df.withColumn("product_name", lower(trim(col("product_name"))))
          .withColumn("category", lower(trim(col("category"))))
    )

def re_price_name(source: str ,df: DataFrame):
  if source == "data_from_db":
    df = df.withColumn("price",regexp_replace(trim(col("price")), "[$\r]", "").cast("float"))
  return df
