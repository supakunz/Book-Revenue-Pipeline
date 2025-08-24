"""
Environment-specific configurations
"""

ENVIRONMENTS = {
    "dev": {
        "spark_master": "local[*]",
        "executor_memory": "1g",
        "driver_memory": "1g",
        "executor_core": "2",
        "s3_endpoint": "http://minio:9000"
    },
    "staging": {
        "spark_master": "spark://spark-master:7077", 
        "executor_memory": "2g",
        "driver_memory": "2g",
        "executor_core": "2",
        "s3_endpoint": "http://minio:9000"
    },
    "production": {
        "spark_master": "spark://spark-prod:7077",
        "executor_memory": "4g", 
        "driver_memory": "2g",
        "executor_core": "2",
        "s3_endpoint": "s3a://prod-bucket"
    }
}

def get_env_config(env: str = "dev"):
    return ENVIRONMENTS.get(env, ENVIRONMENTS["dev"])