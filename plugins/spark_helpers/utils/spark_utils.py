"""
Spark utility functions
"""
from plugins.spark_helpers.configs.spark_configs import SparkConfigs
from plugins.spark_helpers.configs.environment_configs import get_env_config

def create_spark_submit_config(env: str = "dev", app_specific_configs: dict = None):
    """
    สร้าง configuration สำหรับ SparkSubmitOperator
    """
    env_config = get_env_config(env)
    spark_config = SparkConfigs.get_complete_config(
        env=env, 
        master=env_config["spark_master"]
    )
    
    # เพิ่ม memory configs
    spark_config.update({
        'spark.executor.memory': env_config["executor_memory"],
        'spark.driver.memory': env_config["driver_memory"],
        'spark.driver.core': env_config["executor_core"]
    })
    
    # เพิ่ม app-specific configs ถ้ามี
    if app_specific_configs:
        spark_config.update(app_specific_configs)
    
    return spark_config