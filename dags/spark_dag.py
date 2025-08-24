from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from datetime import datetime, timedelta
import yaml
from plugins.spark_helpers.utils.spark_utils import create_spark_submit_config

with open("/opt/airflow/config/config.yaml") as f:
    cfg = yaml.safe_load(f)

# สร้าง config สำหรับ environment นี้
spark_conf = create_spark_submit_config(
    env="staging", # หรือ "staging", "production"
    app_specific_configs={
        'spark.sql.shuffle.partitions': '100' # app-specific config
    }
)

with DAG(
    dag_id="book_revenue_pipeline",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={'owner': 'airflow'}
) as dag:

    extract_from_api = SparkSubmitOperator(
        task_id="extract_from_api",
        application= cfg["paths"]["airflow"]["spark_jobs"]["extract"]["extract_from_api"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf= spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--url", cfg["api"]["airflow"]["url"],
            "--save_path", cfg["paths"]["airflow"]["raw"]["api_file"]["api_path"],
            "--file_name", cfg["paths"]["airflow"]["raw"]["api_file"]["api_file_name"]
    ],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=60),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 3 นาที ก่อน retry
    )

    extract_from_db = SparkSubmitOperator(
        task_id="extract_from_db",
        application= cfg["paths"]["airflow"]["spark_jobs"]["extract"]["extract_from_db"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf= spark_conf,
        packages=(""
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367,"
            "io.delta:delta-core_2.12:2.4.0,"
            "mysql:mysql-connector-java:8.0.33" # ใช้ในการอ่านค่าจาก MySQL database
            ),
        application_args=[
            "--jdbc_url", cfg["database"]["jdbc_url"],
            "--user", cfg["database"]["user"],
            "--password", cfg["database"]["password"],
            "--table", cfg["database"]["table"],
            "--save_path", cfg["paths"]["airflow"]["raw"]["db_file"]["db_path"],
            "--file_name", cfg["paths"]["airflow"]["raw"]["db_file"]["db_file_name"]],
        verbose=True, # ให้แสดง log
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )   

    db_clean = SparkSubmitOperator(
        task_id="db_clean",
        application= cfg["paths"]["airflow"]["spark_jobs"]["transform"]["data_clean"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--source", "data_from_db",
            "--read_path", cfg["paths"]["airflow"]["raw"]["db_file"]["db_path"],
            "--save_path", cfg["paths"]["airflow"]["processed"]["path"],
            "--file_name", cfg["paths"]["airflow"]["raw"]["db_file"]["db_file_name"]],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )

    api_clean = SparkSubmitOperator(
        task_id="api_clean",
        application= cfg["paths"]["airflow"]["spark_jobs"]["transform"]["data_clean"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--source", "data_from_api",
            "--read_path", cfg["paths"]["airflow"]["raw"]["api_file"]["api_path"],
            "--save_path", cfg["paths"]["airflow"]["processed"]["path"],
            "--file_name", cfg["paths"]["airflow"]["raw"]["api_file"]["api_file_name"]],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )

    
    join_table = SparkSubmitOperator(
        task_id="join_table",
        application= cfg["paths"]["airflow"]["spark_jobs"]["transform"]["join_table"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--db_file_name", cfg["paths"]["airflow"]["processed"]["file_name"]["db_file_name"],
            "--db_path", cfg["paths"]["airflow"]["processed"]["path"],
            "--api_file_name", cfg["paths"]["airflow"]["processed"]["file_name"]["api_file_name"],
            "--api_path", cfg["paths"]["airflow"]["processed"]["path"],
            "--save_path", cfg["paths"]["airflow"]["processed"]["path"],
            ],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )


    data_handle_missing = SparkSubmitOperator(
        task_id="handle_missing",
        application= cfg["paths"]["airflow"]["spark_jobs"]["transform"]["handle_missing"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--file_name",'join_table',
            "--read_path", cfg["paths"]["airflow"]["processed"]['path'],
            "--save_path", cfg["paths"]["airflow"]["processed"]["path"]],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )


    data_handle_missing_final = SparkSubmitOperator(
        task_id="handle_missing_final",
        application= cfg["paths"]["airflow"]["spark_jobs"]["transform"]["handle_missing_final"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--read_path", cfg["paths"]["airflow"]["processed"]['path'],
            "--save_path", cfg["paths"]["airflow"]["processed"]["path"]],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )
     

    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application= cfg["paths"]["airflow"]["spark_jobs"]["transform"]["transform"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--read_path", cfg["paths"]["airflow"]["processed"]['path'],
            "--save_path", cfg["paths"]["airflow"]["processed"]["path"]],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )

    
    load_data = SparkSubmitOperator(
        task_id="load_data",
        application= cfg["paths"]["airflow"]["spark_jobs"]["load"]["load_data"], # path ของ Spark job
        conn_id="spark_default", # ชื่อ spark connection ที่เชือมกับ Airflow
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0",
        application_args=[
            "--read_path", cfg["paths"]["airflow"]["processed"]['path'],
            "--save_path", cfg["paths"]["airflow"]["output"]["path"]],
        verbose=True, # ให้แสดง log
        execution_timeout=timedelta(minutes=30),
        retries=3, # ให้ retry 3 ครั้ง
        retry_delay=timedelta(minutes=3) # ให้ delay 5 นาที ก่อน retry
    )

    # กําหนด dependency ของ task

    extract_from_db >> db_clean
    extract_from_api >> api_clean
    
    [db_clean, api_clean] >> join_table
    
    join_table >> data_handle_missing  >> data_handle_missing_final >> transform_data >> load_data
 

