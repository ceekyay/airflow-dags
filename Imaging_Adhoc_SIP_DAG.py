from datetime import datetime, timedelta
from builtins import range
from airflow.models import DAG
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.ssh_operator import SSHOperator

args = {
    'owner': 'airflow',
    'provide_context': True,
    'start_date': datetime(2019, 11, 20),
}

dag = DAG(
    dag_id='Imaging_Adhoc_SIP_Workflow',
    default_args=args,
    schedule_interval=None,
    catchup=False,
)

Ingestion = BashOperator(
  task_id='Ingestion',
  bash_command="echo {{ dag_run.conf['ingestion_status'] }}",
  dag=dag,
)

VirusCheck = BashOperator(
  task_id='VirusCheck',
  bash_command="echo {{ dag_run.conf['quarantine_bucket'] }}",
  dag=dag,
)

MoveToPrecurated = SSHOperator(
  task_id='MoveToPrecurated',
  ssh_conn_id='flywheel',
  command="aws s3 cp s3://{{ dag_run.conf['quarantine_bucket'] }}/ s3://{{ dag_run.conf['precurated_bucket'] }}/ --recursive",
  dag=dag,
)

FlywheelUpload = BashOperator(
  task_id='FlywheelUpload',
  bash_command="echo {{ dag_run.conf['fw_group'] }} && echo {{ dag_run.conf['fw_project'] }} && echo {{ dag_run.conf['fw_template'] }}",
  dag=dag,
)

SuccessNotification = BashOperator(
  task_id="SuccessNotification",
  bash_command="echo {{ dag_run.conf['email_list'] }}",
  dag=dag,
)

Ingestion >> VirusCheck >> MoveToPrecurated >> FlywheelUpload >> SuccessNotification
