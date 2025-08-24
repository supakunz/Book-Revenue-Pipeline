"""
Centralized Spark Configurations
"""
from airflow.models import Variable

class SparkConfigs:
    @staticmethod
    def get_base_config():
        """Base Spark configuration สำหรับทุก job"""
        return {
            'spark.driver.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
            'spark.executor.extraJavaOptions': '-Dcom.amazonaws.services.s3.enableV4=true',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
        }
    
    @staticmethod 
    def get_s3_config(env: str = "dev"):
        """S3/MinIO configuration"""
        if env == "production":
            return {
                'spark.hadoop.fs.s3a.access.key': Variable.get("PROD_S3_ACCESS_KEY"),
                'spark.hadoop.fs.s3a.secret.key': Variable.get("PROD_S3_SECRET_KEY"),
                'spark.hadoop.fs.s3a.endpoint': Variable.get("PROD_S3_ENDPOINT")
            }
        else:
            return {
                'spark.hadoop.fs.s3a.access.key': 'minioadmin',
                'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
                'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000'
            }
    
    @staticmethod
    def get_s3_performance_config():
        """S3 performance และ timeout settings"""
        return {
        # Basic S3A filesystem
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    }
    
    @staticmethod
    def get_complete_config(env: str = "dev", master: str = "local[*]"):
        """รวม config ทั้งหมด"""
        config = {}
        config.update({'spark.master': master})
        config.update(SparkConfigs.get_base_config())
        config.update(SparkConfigs.get_s3_config(env))
        config.update(SparkConfigs.get_s3_performance_config())
        return config