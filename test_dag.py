from datetime import timedelta, date
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Define constants
current_date = str(date.today())
BUCKET = "dsjan2025"
PROJECT_ID = "test-112175"
REGION = "asia-east1"
CLUSTER_NAME_TEMPLATE = "ephemeral-spark-cluster-{{ ds_nodash }}"
PYSPARK_SCRIPT = "gs://dsjan2025/spark-job/flights-etl.py"

DEFAULT_DAG_ARGS = {
    'owner': "airflow",
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': PROJECT_ID,
}

PYSPARK_JOB = {
    "placement": {
        "cluster_name": CLUSTER_NAME_TEMPLATE
    },
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_SCRIPT
    }
}

def list_and_delete_objects(bucket_name, prefix):
    """List and delete objects from subfolders within a given prefix in a GCS bucket."""
    hook = GCSHook()
    objects = hook.list(bucket_name=bucket_name, prefix=prefix)
    if objects:
        for obj in objects:
            # Ensure we're not deleting the prefix itself
            if obj != prefix:
                hook.delete(bucket_name=bucket_name, object_name=obj)


# Define the DAG
with DAG(
    "flights_delay_new",
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval="30 2 * * *",
    catchup=False,
) as dag:

    # Task: Create Dataproc Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME_TEMPLATE,
        region=REGION,
        project_id=PROJECT_ID,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-1",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 30,
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-1",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 30,
                },
            },
            "software_config": {
                "image_version": "1.5-debian10",
            },
        },
    )

    # Task: Submit PySpark job
    submit_pyspark = DataprocSubmitJobOperator(
        task_id="run_pyspark_etl",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Task: Load delays by distance into BigQuery
    bq_load_delays_by_distance = GCSToBigQueryOperator(
        task_id="bq_load_avg_delays_by_distance",
        bucket=BUCKET,
        source_objects=[f"flights_data_output/{current_date}_distance_category/part-*"],
        destination_project_dataset_table=f"{PROJECT_ID}.data_analysis.avg_delays_by_distance",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=0,
        max_bad_records=0
    )

    # Task: Load delays by flight numbers into BigQuery
    bq_load_delays_by_flight_nums = GCSToBigQueryOperator(
        task_id="bq_load_delays_by_flight_nums",
        bucket=BUCKET,
        source_objects=[f"flights_data_output/{current_date}_flight_nums/part-*"],
        destination_project_dataset_table=f"{PROJECT_ID}.data_analysis.avg_delays_by_flight_nums",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=0,
        max_bad_records=0
    )

    # Task: Delete Dataproc Cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_name=CLUSTER_NAME_TEMPLATE,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
        project_id=PROJECT_ID
    )

    # Task: Delete transformed files from GCS
    delete_transformed_files = PythonOperator(
        task_id="delete_transformed_files",
        python_callable=list_and_delete_objects,
        op_kwargs={
            "bucket_name": BUCKET,
            "prefix": "flights_data_output/",
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Set task dependencies
    create_cluster >> submit_pyspark
    submit_pyspark >> [bq_load_delays_by_distance, bq_load_delays_by_flight_nums]
    [bq_load_delays_by_distance, bq_load_delays_by_flight_nums] >> delete_cluster
    delete_cluster >> delete_transformed_files
