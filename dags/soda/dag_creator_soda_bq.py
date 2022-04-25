import os
from datetime import datetime

import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.task_group import TaskGroup
from composer.env import IS_PROD

# Timezone
local_tz = pendulum.timezone("Europe/Paris")

PROJECT = os.getenv("GCP_PROJECT", "local") 

if PROJECT == "my-gcp-project-name-PROD": 
    ENV = "PROD" 
    print("do_something in prod")
elif PROJECT == "my-gcp-project-name-DEV": 
    ENV = "DEV" 
    print("do_something in dev")
else:
    ENV = "LOCAL" 
    print("do_something locally")

DAGS_DIR = os.getenv("DAGS_FOLDER", "./dags") # for composer = /home/airflow/gcs/dags

try:
    with open(f"{DAGS_DIR}/soda/datasources.yml", "r") as f:
        doc = yaml.load(f, Loader=yaml.SafeLoader)
except FileNotFoundError:
    print("datasources config file not found. Place in dags/soda directory")


bq_assets = doc["bigquery"]


def get_warehouse_connection(gcp_project: str, dataset: str) -> dict:
    """Assembles a warehouse.yml as dictionary for soda to
        to be used as connection details to a datasoure

    Args:
        project ([str]): gcp project name
        dataset ([str]): dataset name, e.g. POS

    Returns:
        [dict]: [warehouse.yml containing connection details to datasource]
    """
    bq_conn_id = f"soda_bq_{gcp_project}_{ENV}"
    connection = GoogleBaseHook(gcp_conn_id=bq_conn_id)
    bq_keyfile_json = connection._get_field("keyfile_dict")
    project_id = connection._get_field("project")
    warehouse_conn = {
        "name": "bq_conn_default",
        "connection": {
            "type": "bigquery",
            "account_info_json": f"{bq_keyfile_json}",
            "project_id": project_id,
            "auth_scopes": [
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
            ],
            "dataset": f"{dataset}",
        },
    }
    return warehouse_conn


def _run_soda_scan(warehouse_yml: dict, scan_yml: str) -> None:
    """runs a soda sql test case on a defined data source

    Args:
        warehouse_yml (dict): Warehouse yaml containing connection details
            for data source to be tested
        scan_yml (str): Path to a scan yaml file (soda test case definition)

    Raises:
        ValueError: If test case has not successfully passed

    Returns:
        None: No returns
    """
    import json

    from sodasql.scan.scan_builder import ScanBuilder

    scan_builder = ScanBuilder()
    """Optionally you can directly build the warehouse dict from
    Airflow secrets/variables and set scan_builder.warehouse_dict
    with values."""
    scan_builder.warehouse_yml_dict = warehouse_yml
    scan_builder.scan_yml_file = scan_yml
    scan_builder.variables = {"date": "{{ds_nodash}}"}
    scan = scan_builder.build()
    scan_result = scan.execute()
    test_results = scan_result.to_dict()["testResults"]
    print(json.dumps(test_results, indent=4))
    if scan_result.has_test_failures():
        failures = scan_result.get_test_failures_count()
        raise ValueError(f"Soda Scan found {failures} errors in your data!")
    else:
        print("Success: No errors found in Soda scan")
    return None


def get_scan_yamls(scan_yaml_directory: str, table: str) -> list:
    """Assembles list of absolute paths to scan.yml files

    Args:
        scan_yaml_directory (str): Path to a directory containing
            scan.yml files
        table (str): Name of table to be tested, as in datasource

    Returns:
        list: Contains absolute paths to scan.yml files
    """
    from pathlib import Path

    scan_ymls_list = [
        str(path)
        for path in Path(f"{scan_yaml_directory}/{table}").glob("*.yml")
    ]
    return scan_ymls_list


def generate_soda_task_bigquery(
    project: str, dataset: str, warehouse_yml: dict, scan_yml_path: str
) -> PythonVirtualenvOperator:
    """Generates an airflow task for a single soda test case against a
        defined data source

    Args:
        project (str): Name of gcp project of datasource the test
            is run against
        dataset (str): Name of dataset the test is run against,
            as in datasource
        warehouse_yml (dict): Warehouse yaml containing connection details
            for data source to be tested
        scan_yml_path (str): Path to a scan yaml file (soda test definition)

    Returns:
        PythonVirtualenvOperator: Airflow operator used to run soda test
    """
    table = scan_yml_path.split("/")[-1].replace(".yml", "")
    soda_sql_scan_op = PythonVirtualenvOperator(
        # < Project >< Dataset >_<Test_File_Name>
        task_id=f"soda_test_{project}_{dataset}_{table}",
        python_callable=_run_soda_scan,
        requirements=[
            "soda-sql-core==2.1.3",
            "MarkupSafe==2.0.1",
            "soda-sql-bigquery==2.1.3",
        ],
        system_site_packages=False,
        op_kwargs={"warehouse_yml": warehouse_yml, "scan_yml": scan_yml_path},
    )
    return soda_sql_scan_op


def generate_dummy_task(id_prefix: str, table: str) -> DummyOperator:
    """Generates an Airflow DummyOperator

    Args:
        id_prefix (str): prefix for task id
        table (str): Name of a table in datasource

    Returns:
        [DummyOperator]: Airflow task dummy
    """
    dummy = DummyOperator(task_id=f"{id_prefix}_scan_{table}")
    return dummy


default_args = {
    "owner": "soda_sql",
    "retries": 0,
    "start_date": datetime(2022, 2, 10, tzinfo=local_tz),
}


with DAG(
    dag_id="soda_bigquery",
    default_args=default_args,
    description="Soda SQL Data Quality scan DAG for bigquery",
    schedule_interval="45 7 * * *",
    catchup=False,
    tags=["dataquality"],
) as dag:
    start_tests = DummyOperator(task_id="start_dag")
    end_tests = DummyOperator(task_id="end_dag")

    task_groups = []
    for project in bq_assets:
        for dataset in bq_assets[project]:
            warehouse_yml = get_warehouse_connection(project, dataset)
            scan_yaml_dir = f"{DAGS_DIR}/soda/bigquery/{project}/{dataset}/tables"
            dag_desc = f"Soda data quality scans for bigquery \
                {project}-{ENV}.{dataset}"
            with TaskGroup(
                f"tests_{project}-{ENV}_{dataset}", tooltip=dag_desc
            ) as tg:
                """loop through files in each table directory and construct
                a task for each file"""
                for table in bq_assets[project][dataset]:
                    scan_ymls_list = get_scan_yamls(scan_yaml_dir, table)
                    all_tasks_current_table = [
                        generate_soda_task_bigquery(
                            project, dataset, warehouse_yml, scan_yml
                        )
                        for scan_yml in scan_ymls_list
                    ]
                    start_dummy = generate_dummy_task("start", table)
                    end_dummy = generate_dummy_task("end", table)
                    start_dummy >> all_tasks_current_table >> end_dummy
                    task_groups.append(tg)

    start_tests >> task_groups >> end_tests
