from airflow.models import DAG
from datetime import timedelta,datetime
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.dates import days_ago
import csv
from airflow.operators.python import PythonOperator

# def run_ssis_package(**kwargs):
#     pass

# default_args = {
#     'owner':'Shamen',
#     'start_date' : days_ago(0),
#     'email':['shamen_paris@next.co.uk'],
#     'retries':1,
#     'retry_delay': timedelta(minutes=5),
# }
# with DAG('sftp_source_to_target', default_args = default_args, schedule_interval=None) as dag:
#    get_package = SFTPOperator(task_id='get_package',
#                                   ssh_conn_id='ssis_path',
#                                   remote_filepath = "https://hoflandingzone.dfs.core.windows.net/packages/Package.dtsx",
#                                   local_filepath = "/opt/airflow/{{run_id}}/Package.dtsx",
#                                   operation="get",
#                                   create_intermediate_dirs=True
#                                  )
   
#    run_package = PythonOperator(task_id="run_ssis_package",
#                                  templates_dict={
#                                     "input_file": "/opt/airflow/{{run_id}}/Package.dtsx",
#                                     "output_file": "/opt/airflow/{{run_id}}/output.csv"
#                                  },
#                                  python_callable=run_ssis_package
#                                  )
#    get_package >> run_package