---
title: "Data Quality Checks with Soda in Airflow"
date: 2022-04-25T10:40:00+01:00
tags:
    - python
    - dataquality
    - airflow
    - cloud composer
categories:
    - dataengineering
keywords:
    - data quality
    - airflow
    - cloud composer
    - gcp
draft: true
---

# Setup Data Quality Checks with Soda in Airflow

## Summary  

You'll learn how to set up a process that integrates soda data quality checks to an airflow DAG.
This allows for data quality tests to be automatically scheduled and run by airflow once created.
This example covers testing data sources in postgres. Code for data sources in bigquery can be found in my repository. 

### Repository

The whole project can be found on [github](https://github.com/cygandev/todo)

### Features

* Write data quality tests against data in postgres and bigquery
* Automatically integrate data quality tests in Airflow DAG
* Run data quality tests on a schedule
* Alert when data quality tests fail

### Limitations

* Currently only set up for data sources in postgres and bigquery
* All tests run on the same schedule

### Stack

* Python
* Soda SQL
* Airflow
* Cloud Composer
* BigQuery
* Postgres

## Use Case

Data quality tests are very easy to set up with Soda SQL. However, less technical co-workers like data analysts might find it intimidating to set up and configure a DAG in Airflow for a test to be run on a schedule.
With the framework introduced in this post, all they have to do is to write the soda test and drop it to a directory. Deployment on airflow is then taken care of.

## Project struture

We suggest to structure the project as follows.

```bash
soda 
├── bigquery
│ ├── <gcp-project>
│ │ ├── <dataset>                     'All tests for a specific BQ dataset'
│ │ │ └── tables                      
│ │ │ └── <table>                     'All tests for a specific table'
│ │ │   └── <test>.yml                'All scan YAMLs for table'
├── postgres
│ ├── <schema>                        'All tests for a specific postgres schema'
│ │ └── tables
│ │   └── <table>                     'All tests for a specific table'
│ │		└── <test>.yml                'All scan YAMLs for table'
├── dag_creator_soda_bq.py            'DAG definition for bigquery'
├── dag_creator_soda_pg.py            'DAG definition for postgres'
├── datasources.yml                   'Datasources config file'
```

## Soda SQL

I love Soda SQL. It's really easy to set up data quality tests against data sources like Postgres, BigQuery, Snowflake, etc.
Tests are defined in YAML files and can be directly run with the Soda SQL CLI or deployed in Airflow [1](https://docs.soda.io/soda-sql/overview.html).

A Soda data quality test requires two components:

### Warehouse YAML

Configure a connection to the data source the test is to be run against.

```yaml
name: conn_to_my_postgres
connection:
  type: postgres
  host: localhost
  username: <myuser>
  password: <mypassword>
  database: sodasql
  schema: public
```

### Scan YAML

Soda SQL comes with a set of pre defined metrics like `row_count` that can be used in the test definition.
The test criteria is defined against a metric with operators like `<`, `>` or `==`.
In the example below the test simply requires the table `customers` to have a row count greater than 0.
```yaml
table_name: customers
metrics:
  - row_count
tests:
  - row_count > 0
```
In addition to pre-defined metrics it is also possible to set up tests against custom metrics with sql statements. Values defined in the select statement can be referenced in a test as metric. Below the metric is `n_trx_yesterday`, counting all entries in the table with a timestamp from the previous day. As can be seen under tests, the metric `n_trx_yesterday` is required to take a value greater than 0.

```yaml
table_name: transactions
sql_metrics:
  - sql: |
      SELECT
         count(*) as n_trx_yesterday,
      FROM wholesale.transactions
      WHERE Date = current_date() -1
    tests:
      - n_trx_yesterday > 0
```

### Run a test
A test can easily be run with the Soda SQL CLI by providing the path to the test definition and the corresponding warehouse.yml with connection instructions to the data source.

```bash
soda scan warehouse.yml tables/transactions.yml
```
The output then indicates whether the test was successfull.

```
  | 1 test executed
  | All is good. No tests failed.
  | Exiting with code 0
```

### More information
Now that we have a basic understanding of what Soda SQL is and offers, let's move on to the implementation of the airflow integration. 
However, if you need more information, I highly recommend consulting the official [Soda SQL documentation](https://docs.soda.io/soda-sql/overview.html) or going through the [5 minutes quick start guide](https://docs.soda.io/soda-sql/5_min_tutorial.html) in order to get up to speed quickly.
I found the documentation very well structured and informative.

## Airflow Integration
Like the example from the introduction above indicated, a Soda test consists of following components:
1. Datasources configuration and connections (Wahrehouse YAMLS)
2. Data quality test definitions (Scan YAMLS)
3. Test runner


Let's go through an example for postgres data sources. 


### Datasources Configuration

At the moment, Soda doesn't ship with the possibility to set up a Warehouse YAML for multiple databases or schemas in one place.
Our tests should run data quality checks on tables in different databases or schemas however, and we certainly don't want to define a new Warehouse YAML for each data source manually.

Luckily, apart from a YAML file, the connection details to a warehouse / database can also be specified programmatically and passed to Soda as a dictionary.

The following function takes the name of the database and schema as arguments and assembles a dictionary with the connection details to said database.

Sensitive information such as password and username are taken from an [airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html). When setting the connections up in the airflow UI, please make sure to follow the naming convetion `<database-type>_<database-name>_<environment>` e.g. `postgres_zoo_production`.

This way, we only need one airflow connection per database. Make sure that the user has sufficient rights for all schemas you want to run tests on. 

```python
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
```
Now that we have the means to configure our database connections programmatically, we can define all our data sources just once in a config file an pass it to the function above.

When adhering to the structure, airflow can loop through the following yaml datasources config file and set up a Soda warehouse dictionary containing connection details for all data sources to be tested.

```yaml
postgres:
  #database:
  # schema: [table1, table2]
  zoo:
    animals: [mammals, fish]
    employees: [name, contact_information, salary]
  shop:
    products: [id, price]

bigquery:
  # project:
  #   dataset: [table1, table2]
  my-gcp-project: 
    DWH: [revenue, visitors, expense]
  # my-gcp-project2:
    # dataset: [table1, table2]
```

In order for this to work, the config file must be placed in airflow's `dags` directory in a `soda` subdirectory.

As we're using cloud composer, the absolute directory path for the dags directory is as defined in the pre-defined `DAGS_FOLDER` environment variable.

If you're just testing it on a local airflow instance, place the datasources config file in the directory `dags/soda/datasources.yml`.


```python
PROJECT = os.getenv("GCP_PROJECT", "local") 

if PROJECT == "my-gcp-project-name-PROD": 
    airflow_env = "PROD_ENVIRONMENT" 
    print("execute tests in prod")
elif PROJECT == "my-gcp-project-name-DEV": 
    airflow_env = "DEV_ENVIRONMENT" 
    print("execute tests in dev")
else:
    airflow_env = "LOCAL" 
    print("execute tests locally")

DAGS_DIR = os.getenv("DAGS_FOLDER", "./dags") # for composer = /home/airflow/gcs/dags

# Timezone
local_tz = pendulum.timezone("Europe/Paris")

try:
    with open(f"{DAGS_DIR}/soda/datasources.yml", "r") as f:
        doc = yaml.load(f, Loader=yaml.SafeLoader)
except FileNotFoundError:
    print("Datasources config file not found. Place in dags/soda directory")

# load all postgres data sources from config
pg_assets = doc["postgres"]
```
Now that we have our data sources configured and connections set up, we can move on to define data quality tests on our data.

### Data Quality Test Definitions

As mentioned in the introduction, Soda SQL requires data quality tests against data to be defined in a scan YAML file like the example below. Specify the table's name, the metric to be measured and the test criteria to be evaluated.

```yaml
table_name: customers
metrics:
  - row_count
tests:
  - row_count > 0
```

This step is mostly carried out by a data analyst. When the scan yaml is defined, the test must be placed in the right directory. Please set up the project structure as suggested above and drop the scan yaml in the following directory:
```bash
soda 
├── postgres
│ ├── <schema>                        'All tests for a specific postgres schema'
│ │ └── tables
│ │   └── <table>                     'All tests for a specific table'
│ │		└── <test>.yml                '--> Place scan YAMLs for this table HERE'
```
Or as absolute path `dags/soda/postgres/<db>/<schema>}/tables/<table>/<scan yamls>`.

### Test Runner

While we have a rather deep project directory structure, we can provide the test runner in airflow with a reproducible path to the scan yamls location for each table.

Below, a function is defined that takes as arguments `warehouse_yml` containing the connection details to a data source and `scan_yml` containg the test definition against this exact data source.

```python
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
```

We want the function to raise an error when a data quality test was not executed successfuly.
See the official [Soda documentary](https://docs.soda.io/soda-sql/programmatic_scan.html) for more details about programmatic scans.

The function above is then passed to an airflow operator to execute it in a task as part of a a DAG.
We want one task per Soda data quality test.

The below function generates a task with a `PythonVirtualenvOperator` that takes a function (python_callable) as an argument. In our case, the callable is the function we defined earlier `_run_soda_scan`.

```python
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
```

## DAG Generation

We have introduced logic to handle all necessary components to run a Soda data quality check against a data source. Let's now define a workflow that takes alle defined Soda tests an runs them automatically on a schedule.

For this, we let Airflow loop through the scan yaml files in our directory structure, create a task for each scan yaml and assemble a DAG containing every test:


```python
default_args = {
    "owner": "soda_sql",
    "retries": 0,
    "start_date": datetime(2022, 4, 25, tzinfo=local_tz),
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
```

There are to helper functions invovled that have no been introduced yet.

`get_scan_yamls` defines the logic to loop through the scan yaml files.

```python
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
```

`generate_dummy_task` allows to easily generate placeholder tasks with the DummyOperator.
```python
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
```

We now have set up a DAG that runs every day on 7:30 an executes all defined data quality checks and raises an error for each failed test.

![DAG Postgres](/images/airflow_dag.gif "DAG Postgres")

### Test Directories
To avoid having to create test directories manually, we created the following scripts. We run them at the end of our CI/CD Pipeline.

```python
import sys
import yaml


def assemble_all_table_paths(datasources_yaml: str) -> str:
    with open(datasources_yaml, "r") as f:
        doc = yaml.load(f, Loader=yaml.SafeLoader)  # also, yaml.FullLoader
    all_tables = []
    for db_type in doc:
        if db_type == "postgres":
            for db in doc[db_type]:
                for db_schema in doc[db_type][db]:
                    for table in doc[db_type][db][db_schema]:
                        all_tables.append(
                            f"{db_type}/{db}/{db_schema}/tables/{table}"
                        )
        elif db_type == "bigquery":
            for project in doc[db_type]:
                for dataset in doc[db_type][project]:
                    for table in doc[db_type][project][dataset]:
                        all_tables.append(
                            f"{db_type}/{project}/{dataset}/tables/{table}"
                        )
    all_tables_bash_array = " ".join(all_tables)
    print(all_tables_bash_array)
    return


assemble_all_table_paths(sys.argv[1])
```
And execute it in bash.

```bash
# create test directories
export SODA_DIR='dags/soda'
export DATASOURCES_YML_PATH='dags/soda/datasources.yml'

for dir in `python3 ./utils/dir_creator.py ${DATASOURCES_YML_PATH}`; do \
    mkdir -p ./${SODA_DIR}/$dir; \
done
# End
```

## Deploy
Pretty easy. Just place the DAG generator python file in the `soda` subdirectory of Airflow's `dags` directory.

```bash
dags/soda 
├── dag_creator_soda_bq.py            'DAG definition for bigquery'
├── dag_creator_soda_pg.py            'DAG definition for postgres'
```

## Alerts

We have set up alerts on cloud logging that drops us a message in MS Teams and Slack each time a test fails.

## Next Steps

This is our working version 1.0, there's still a lot to improve:
- Currently all tests are run with the same schedule - Make schedule part of config
- Currently seperate dag generator files for postgres and bigquery - Generate from one file
- Connections have to be set up manually in Airflow - use GCP's secret manager


## Feedback

Let me know if this was usefule to you or if you have suggestions for improvements.
