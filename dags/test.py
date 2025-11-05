from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. Define the DAG's default arguments
# These args will be applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),  # Start date for the DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# 2. Instantiate the DAG
# This is the "container" for all your tasks
with DAG(
    dag_id='my_first_simple_dag',          # The name of the DAG in the Airflow UI
    default_args=default_args,             # Apply the default args defined above
    description='A simple tutorial DAG',
    schedule_interval='@daily',            # How often to run the DAG
    catchup=False,                         # Don't run past instances
    tags=['example', 'tutorial'],          # Tags to help filter in the UI
) as dag:

    # 3. Define the tasks (Operators)
    
    # Task 1: A simple Bash command to print "hello"
    task_1 = BashOperator(
        task_id='print_hello',            # The name of the task
        bash_command='echo "Hello!"',     # The command to run
    )

    # Task 2: Another Bash command to print "goodbye"
    task_2 = BashOperator(
        task_id='print_good
