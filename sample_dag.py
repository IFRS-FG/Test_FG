from datetime import datetime, timedelta
import pytz
from dateutil.tz import gettz

from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor

DAG_ID = "sp_ifrs_initial_load"
PROJECT_ID = "fg-ifrs-uat-405809"
REPOSITORY_ID = "fg-ifrs-uat-dataform"
REGION = "asia-south1"

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 5, 14),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with models.DAG(
    DAG_ID,
    schedule_interval="0 8-20 * * *",  # Runs from 8 AM to 8 PM everyday
    start_date=datetime(2023, 5, 14, tzinfo=pytz.timezone('Asia/Kolkata')),
    dagrun_timeout=timedelta(minutes=1),
    catchup=False,  # Override to match your needs
    tags=['dataform_initial_load'],
   ) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/fg-ifrs-uat-405809/locations/asia-south1/repositories/fg-ifrs-uat-dataform/"
                f"workspaces/fg-ifrs-uat-workspace"
		),
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
         workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )
    
    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
    task_id="is_workflow_invocation_done",
    project_id=PROJECT_ID,
    region=REGION,
    repository_id=REPOSITORY_ID,
    workflow_invocation_id=("{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"),
    expected_statuses={WorkflowInvocation.State.SUCCEEDED},
)

create_compilation_result >> create_workflow_invocation >> is_workflow_invocation_done