# Setup Data Quality Checks with Soda in Airflow / Cloud Composer

Set up a process that integrates soda data quality checks to an airflow DAG.
This allows for data quality tests to be automatically scheduled and run by airflow once created.
At the moment, this covers testing data sources in postgres and bigquery. 

## Features

* Write data quality tests against data in postgres and bigquery
* Automatically integrate data quality tests in Airflow DAG
* Run data quality tests on a schedule
* Alert when data quality tests fail

## Project struture

We suggest to structure the project as follows.

```bash
dags/soda 
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

## Concept
1) Define all tables to be tested with soda in datasources.yml
2) Create a directory for each table (see utils for script to handle this automatically)
3) Set up a connection for each database in airflow UI, using follow the naming convetion `<database-type>_<database-name>_<environment>` e.g. `postgres_zoo_production`
4) Define test case(s) for your tables and place in table's corresponding directory
5) Let airflow run your data quality tests

## Deploy

Pretty easy.

1) Configure the data sources to be tested in `datasources.yml`.
```bash
dags/soda 
├── datasources.yml                   'Datasources config file'
```
Stick to the following structure.
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
2) Place the DAG creator python file in the `soda` subdirectory of Airflow's `dags` directory.

```bash
dags/soda 
├── dag_creator_soda_bq.py            'DAG definition for bigquery'
├── dag_creator_soda_pg.py            'DAG definition for postgres'
```

2) Make sure to create the test directory for each table either manually or with the script above.

```bash
dags/soda 
├── bigquery
│ ├── <gcp-project>
│ │ ├── <dataset>                     'All tests for a specific BQ dataset'
│ │ │ └── tables                      
│ │ │ └── <table>                     'All tests for a specific table'
│ │ │   └── <test>.yml                'scan YAMLs for table (test definition)'
├── postgres
│ ├── <schema>                        'All tests for a specific postgres schema'
│ │ └── tables
│ │   └── <table>                     'All tests for a specific table'
│ │		└── <test>.yml                'Scan YAMLs for table (test definition)'
```

3) Write your test definitions and place them in the table's corresponding directory.
4) Let Airflow handle running the tests for you

## More Details

More detailed explanation and a walkthrough on all components can be found on [my blog](https://datastack.ch/posts/soda-airflow-integration/)