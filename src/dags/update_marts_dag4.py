# dags/update_marts_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_geo_marts1',
    default_args=default_args,
    description='Daily update of geo marts',
    schedule_interval='@daily',
    start_date=datetime(2025, 9, 1),
    catchup=False,
)

SPARK_CONF = {
    # --- Driver & AM ---
    "spark.driver.memory": "768m",
    "spark.driver.memoryOverhead": "3g",     # ← увеличено
    "spark.driver.cores": "1",
    
    "spark.yarn.am.memory": "768m",
    "spark.yarn.am.memoryOverhead": "2g",
    "spark.yarn.am.cores": "1",

    # --- Executor ---
    "spark.executor.memory": "1g",
    "spark.executor.memoryOverhead": "2g",   # ← важно!
    "spark.executor.cores": "1",

    # --- SQL ---
    "spark.sql.shuffle.partitions": "10",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",

    # --- Dynamic Allocation ---
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "2",

    # --- Java Options ---
    "spark.driver.extraJavaOptions": "-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled",
    "spark.executor.extraJavaOptions": "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled"
}

build_user_mart = SparkSubmitOperator(
    task_id='build_user_mart',
    application='/lessons/scripts/run_user_mart.py',
    name='user_geo_mart_job',
    conn_id='spark_default',
    verbose=True,
    conf=SPARK_CONF,
    dag=dag
)

build_zone_mart = SparkSubmitOperator(
    task_id='build_zone_mart',
    application='/lessons/scripts/run_zone_mart.py',
    name='zone_geo_mart_job',
    conn_id='spark_default',
    verbose=True,
    conf=SPARK_CONF,
    dag=dag
)

build_friend_mart = SparkSubmitOperator(
    task_id='build_friend_mart',
    application='/lessons/scripts/run_friend_mart.py',
    name='friend_rec_job',
    conn_id='spark_default',
    verbose=True,
    conf=SPARK_CONF,
    dag=dag
)

build_user_mart >> [build_zone_mart, build_friend_mart]
# [build_zone_mart, build_friend_mart]