from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType
from pyspark.sql.functions import when, col, lit

# def remove_outliers(df: DataFrame):
#     """
#     ลบ outliers โดยอัตโนมัติจากทุกคอลัมน์ที่เป็นตัวเลข ด้วย IQR method
#     และ log จำนวนที่ถูกลบ
#     """
#     numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]

#     for column in numeric_cols:
#         q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.01)
#         iqr = q3 - q1
#         lower_bound = q1 - 1.5 * iqr
#         upper_bound = q3 + 1.5 * iqr

#         # Mark outliers
#         df = df.withColumn(
#             f"is_outlier_{column}",
#             when((col(column) < lower_bound) | (col(column) > upper_bound), True).otherwise(False)
#         )

#         # Log จำนวนที่เป็น outlier
#         outlier_count = df.filter(col(f"is_outlier_{column}") == True).count()
#         print(f"[LOG] Removed {outlier_count} outliers from column '{column}'")

#         # Filter เฉพาะค่าไม่ใช่ outlier # เหลืแแค่ข้อมูลที่ไม่ใช่ outlier
#         df = df.filter(col(f"is_outlier_{column}") == False)

#         # Drop flag column
#         df = df.drop(f"is_outlier_{column}")

#     return df


def remove_outliers(df: DataFrame):
    """
     ลบ outliers โดยอัตโนมัติจากทุกคอลัมน์ที่เป็นตัวเลข ด้วย IQR method
     และ log จำนวนที่ถูกลบ
     """
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    
    # เก็บชื่อคอลัมน์ Outlier flags
    outlier_flag_cols = []
    
    for column in numeric_cols:
        q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outlier_flag_col_name = f"is_outlier_{column}"
        outlier_flag_cols.append(outlier_flag_col_name)

        # สร้างคอลัมน์ Flag โดยยังไม่เรียก Action
        df = df.withColumn(
            outlier_flag_col_name,
            when((col(column) < lit(lower_bound)) | (col(column) > lit(upper_bound)), lit(True)).otherwise(lit(False))
        )
    
    # สร้างเงื่อนไขสำหรับ filter
    filter_condition = lit(False)
    for flag_col in outlier_flag_cols:
        filter_condition = filter_condition | col(flag_col)

    # Log จำนวนที่ถูกลบไปทั้งหมด
    outlier_count = df.filter(filter_condition).count()
    print(f"[LOG] Removed {outlier_count} outliers from all numeric columns.")
    
    # กรองข้อมูลและลบคอลัมน์ Flag ทั้งหมดใน Action เดียว
    df = df.filter(~filter_condition).drop(*outlier_flag_cols)
    
    return df