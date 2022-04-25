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