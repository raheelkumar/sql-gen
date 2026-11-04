import os
import sys
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from py.common_helper import get_dag_start_date, create_json_from_bqconfig, read_json
from py.get_env_variable import get_curate_project_id, get_enrich_project_id, get_resource_sa, get_raw_project_id
from py.error_helper import task_failure_log
from py.obserbility_helper import success_log
# ---------------------------------------------------------------------------
# Set variables
# ---------------------------------------------------------------------------
PROJECT_ID_CURATE = get_curate_project_id()
PROJECT_ID_ENRICH = get_enrich_project_id()
PROJECT_ID_RAW = get_raw_project_id()
RESOURCE_SA = get_resource_sa()

PROJECT_SPACE = Variable.get('PROJECT_SPACE')

#DAG_NAME = 'corp_challenger_model_driver_line_item_view_to_table'



dag_configs = ["corp_challenger_model_driver"]
for dag_name in dag_configs:
    DAG_NAME = f'{dag_name}_line_item_view_to_table'

    CONFIG_FILE_PATH = f'dags/{PROJECT_SPACE}/{dag_name}/config/{dag_name}_config.json'
    
    # Create json file from config
    # create_json_from_bqconfig(CONFIG_FILE_PATH, DAG_NAME)

    bq_config_params = read_json(CONFIG_FILE_PATH)

    BQ_CONN_ID = bq_config_params["bq_conn_id"]

    # ---------------------------------------------------------------------------
    # Table Variables Split (Project, Dataset, Table)
    # ---------------------------------------------------------------------------
    # Curate Config
    # CURATE_DATASET = bq_config_params.get("curate_dataset")
    # CURATE_TABLE_NAME = bq_config_params.get("curate_table_name")
    # CONFIG_TABLE = f"{PROJECT_ID_CURATE}.{CURATE_DATASET}.{CURATE_TABLE_NAME}"

    RAW_PROJECT_ID = PROJECT_ID_RAW
    RAW_DATASET = bq_config_params.get("raw_dataset")
    RAW_TABLE_NAME = bq_config_params.get("raw_bq_table")

   

    # Publish Output
    PUBLISH_PROJECT_ID = PROJECT_ID_RAW
    PUBLISH_DATASET = bq_config_params.get("publish_dataset")
    PUBLISH_TABLE_NAME = bq_config_params.get("publish_bq_table")
   

    # DAG_NAME = dag_name

    # CONFIG_FILE_PATH = f'dags/{PROJECT_SPACE}/{DAG_NAME}/config/{DAG_NAME}_config.json'
    
    # # Create json file from config
    # create_json_from_bqconfig(CONFIG_FILE_PATH,DAG_NAME)

    # bq_config_params = read_json(CONFIG_FILE_PATH)

    # BQ_CONN_ID = bq_config_params["bq_conn_id"]
    # CONFIG_TABLE = bq_config_params["curate_bq_table"]#curate #"vz-it-np-j0nv-dev-oddo-0.od_enriched_epmmlops.ChallengerModel_RevenueConfig"
    # HIERARCHY_TABLE = bq_config_params["enrich_bq_table"]#enrich  #"vz-it-np-j0nv-dev-oddo-0.od_enriched_edmcs.edm_master_incl_alt_hierarchies_v"
    # TRANSACTION_TABLE = bq_config_params["raw_bq_table"]#raw #"vz-it-np-j0nv-dev-oddo-0.od_published_epm.epm_p00all_actuals_v"        
    # OUTPUT_TABLE = bq_config_params["publish_bq_table"]# #"<project>.<dataset>.<final_output_table>"              # TODO: fill in

    OPERATIONAL_DATASET = bq_config_params["operational_dataset"]
    bq_conn_id = BQ_CONN_ID  

    schedule_int = bq_config_params["schedule"]

    SQL_FOLDER = 'corp_challenger_model_driver'

    default_args = {
        'start_date': days_ago(1),
        'retries': 2,
        'retry_delay': timedelta (minutes=2),
        'on_failure_callback': task_failure_log,
        'on_success_callback': success_log    }

       # DAG definitions
    with DAG(dag_id=f'{DAG_NAME}',
            catchup=False,
            schedule_interval=schedule_int,
            max_active_runs=1,
            default_args=default_args,
            template_searchpath=os.path.join(os.environ.get('DAGS_FOLDER'),f'{PROJECT_SPACE}/{SQL_FOLDER}/sql'),
            tags= ['view_to_table']
             ) as dag:
            view_to_table = BigQueryInsertJobOperator(
                task_id = f'{DAG_NAME}_view_to_table',
                configuration = {
                    "query": {
                        "query": "{% include 'corp_challenger_model_driver_line_item_view_to_table.sql' %}",
                        "useLegacySql": False
                    }
                },
                params={
                "PROJECT_ID_ENRICH": PROJECT_ID_ENRICH,
                "PUBLISH_DATASET":PUBLISH_DATASET,
                "publish_bq_table": PUBLISH_TABLE_NAME,
                "RAW_DATASET":RAW_DATASET,
                "raw_bq_table": RAW_TABLE_NAME,
                "OPERATIONAL_DATASET": OPERATIONAL_DATASET},
                gcp_conn_id=bq_conn_id
            )
           