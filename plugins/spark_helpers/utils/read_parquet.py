from pyspark.sql import SparkSession

def read_latest_parquet(spark: SparkSession, path_read: str, source: str):
    """
    Finds the latest directory starting with 'source' in the given path
    and reads all parquet files within that directory.
    """
    sc = spark.sparkContext
    URI = sc._jvm.java.net.URI
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    
    fs = FileSystem.get(URI(path_read), sc._jsc.hadoopConfiguration())

    # 1. ลิสต์ object ทั้งหมดใน path
    all_objects = fs.listStatus(Path(path_read))

    # 2. กรองหาเฉพาะ "โฟลเดอร์" ที่ชื่อขึ้นต้นด้วย source ที่ต้องการ
    sub_folders = [
        f.getPath().getName() for f in all_objects if f.isDirectory() and f.getPath().getName().startswith(source)
    ]

    if not sub_folders:
        raise FileNotFoundError(f"No directories starting with '{source}' found in {path_read}")

    # 3. Sort ชื่อโฟลเดอร์เพื่อหาโฟลเดอร์ล่าสุด
    latest_folder = sorted(sub_folders, reverse=True)[0]

    # 4. สร้าง Path เต็มไปยังโฟลเดอร์ล่าสุด
    full_path_to_folder = f"{path_read}/{latest_folder}"
    print(f"Loading latest data from folder: {full_path_to_folder}")
    
    # 5. สั่งให้ Spark อ่านข้อมูลจาก "ทั้งโฟลเดอร์"
    # Spark จะอ่านไฟล์ .parquet ทั้งหมดที่อยู่ในนั้นโดยอัตโนมัติ
    df = spark.read.parquet(full_path_to_folder)
    return df