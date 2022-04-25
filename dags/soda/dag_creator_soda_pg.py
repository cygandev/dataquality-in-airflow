import os
from datetime import datetime

import pendulum
import yaml
from airflow.hooks.base_hook import BaseHook
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.task_group import TaskGroup

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

# Timezone
local_tz = pendulum.timezone("Europe/Paris")

try:
    with open(f"{DAGS_DIR}/soda/datasources.yml", "r") as f:
        doc = yaml.load(f, Loader=yaml.SafeLoader)
except FileNotFoundError:
    print("datasources config file not found. Place in dags/soda directory")

pg_assets = doc["postgres"]


def get_warehouse_connection(database: str, schema: str) -> dict:
    """Assembles a warehouse.yml dictionary for soda to
        to be used as connection details to a warehouse
        https://docs.soda.io/soda-sql/programmatic_scan.html

    Args:
        database ([str]): database name, e.g. dwh
        schema ([str]): schema name, e.g. frequencycam

    Returns:
        [dict]: [warehouse.yml containing connection details to warehouse]
    """
    airflow_conn_id = f"postgres_{database}_{ENV}"
    connection = BaseHook.get_connection(f"{airflow_conn_id}")
    warehouse_conn = {
        "name": f"{airflow_conn_id}",
        "connection": {
            "type": "postgres",
            "host": f"{connection.host}",
            "port": "5432",
            "username": f"{connection.login}",
            "password": f"{connection.password}",
            "database": f"{connection.schema}",
            "schema": f"{schema}",
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
    # scan_builder.variables = {"date": "{{ds_nodash}}"}
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
        scan_yaml_directory(str): Path to a directory containing scan.yml files
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


def generate_soda_task_postgres(
    db: str, schema: str, warehouse_yml: dict, scan_yml_path: str
) -> PythonVirtualenvOperator:
    """Generates an airflow task for a single soda test case against a
        defined data source

    Args:
        db (str): Name of database the test is run against, as in datasource
        schema (str): Name of schema the test is run against, as in datasource
        warehouse_yml (dict): Warehouse yaml containing connection details for
            data source to be tested
        scan_yml_path (str): Path to a scan yaml file
            (soda test case definition)

    Returns:
        PythonVirtualenvOperator: Airflow operator used to run soda test
    """
    table = scan_yml_path.split("/")[-1].replace(".yml", "")
    soda_sql_scan_op = PythonVirtualenvOperator(
        # < DB >_< Schema >_<Test_File_Name>
        task_id=f"soda_test_{db}_{schema}_{table}",
        python_callable=_run_soda_scan,
        requirements=[
            "soda-sql-core==2.1.3",
            "MarkupSafe==2.0.1",
            "soda-sql-postgresql==2.1.3",
        ],
        system_site_packages=False,
        op_kwargs={"warehouse_yml": warehouse_yml, "scan_yml": scan_yml_path},
    )
    return soda_sql_scan_op


def generate_dummy_task(id_prefix, table):
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
    dag_id="soda_postgres",
    default_args=default_args,
    description="Soda SQL Data Quality scan DAG for postgres",
    schedule_interval="30 7 * * *",
    catchup=False,
    tags=["dataquality"],
) as dag:
    start_tests = DummyOperator(task_id="start_dag")
    end_tests = DummyOperator(task_id="end_dag")

    task_groups = []
    for db in pg_assets:
        for schema in pg_assets[db]:
            # set up warehouse connection on schema
            warehouse_yml = get_warehouse_connection(db, schema)
            scan_yaml_dir = f"{DAGS_DIR}/soda/postgres/{db}/{schema}/tables"
            dag_desc = f"Soda data quality scans for postgres {db}.{schema}"
            with TaskGroup(f"tests_{db}_{schema}", tooltip=dag_desc) as tg:
                """loop through files table directory and construct
                a task for each file"""
                for table in pg_assets[db][schema]:
                    scan_ymls_list = get_scan_yamls(scan_yaml_dir, table)
                    all_tasks_current_table = [
                        generate_soda_task_postgres(
                            db, schema, warehouse_yml, scan_yml
                        )
                        for scan_yml in scan_ymls_list
                    ]
                    start_dummy = generate_dummy_task("start", table)
                    end_dummy = generate_dummy_task("end", table)
                    start_dummy >> all_tasks_current_table >> end_dummy
                    task_groups.append(tg)

    start_tests >> task_groups >> end_tests
