from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess
import sys

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'run_python_script_15min',
    default_args=default_args,
    description='Run external Python script every 15 minutes',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['python', 'script'],
)

# Option 1: Run script directly with bash (simpler)
run_script_bash = BashOperator(
    task_id='run_script_with_bash',
    bash_command='cd /opt/airflow/scripts && pip install -q -r requirements.txt && python script.py',
    dag=dag,
)

# Option 2: Run script with Python operator (more control)
def run_external_script():
    """
    Function to install dependencies and run the script
    """
    script_dir = '/opt/airflow/scripts'
    requirements_file = f'{script_dir}/requirements.txt'
    script_file = f'{script_dir}/script.py'
    
    # Install dependencies
    print(f"Installing dependencies from {requirements_file}")
    subprocess.check_call([
        sys.executable, '-m', 'pip', 'install', '-q', '-r', requirements_file
    ])
    
    # Run the script
    print(f"Running script: {script_file}")
    result = subprocess.run(
        [sys.executable, script_file],
        cwd=script_dir,
        capture_output=True,
        text=True
    )
    
    print(f"Script output: {result.stdout}")
    if result.stderr:
        print(f"Script errors: {result.stderr}")
    
    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}")
    
    return result.stdout

# Uncomment this if you prefer the Python operator
# run_script_python = PythonOperator(
#     task_id='run_script_with_python',
#     python_callable=run_external_script,
#     dag=dag,
# )

# Set task dependencies (use only one method)
run_script_bash
