from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

# Define cluster config (for job runs)
new_cluster = {
    'spark_version': '14.3.x-scala2.12',   # adjust to your cluster runtime
    'node_type_id': 'Standard_F4',     # Azure node type example
    'num_workers': 1
}

# Notebook tasks config
extract_notebook_task = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Workspace/Project/Extract'
    }
}

transform_notebook_task = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Workspace/Project/Transform'
    }
}

load_notebook_task={
    'new_cluster':new_cluster,
    'notebook_task':{
        'notebook_path':'/Workspace/Project/Load'
    }
}
@dag(
    dag_id='orchestration_retail_project',
    start_date=datetime(2025, 8, 22),  
    schedule='@daily',
    catchup=False
)
def RetailStore_etl_Pipeline():

    extract = DatabricksSubmitRunOperator(
        task_id='extract_task',
        json=extract_notebook_task,
        databricks_conn_id='databricks_default'
    )

    transform = DatabricksSubmitRunOperator(
        task_id='transform_task',
        json=transform_notebook_task,
        databricks_conn_id='databricks_default'
    )

    load=DatabricksSubmitRunOperator(
        task_id='load_task',
        json=load_notebook_task,
        databricks_conn_id='databricks_default'
    )
    # Dependencies
    extract >> transform >>load


# Instantiate DAG
etl_dag = RetailStore_etl_Pipeline()
