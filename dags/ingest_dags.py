from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define los argumentos predeterminados y la fecha de inicio
default_args = {
    "owner": "kremlin",
    "start_date": days_ago(1),
    "schedule_interval": timedelta(days=7)  # Se ejecutará una vez a la semana
}

# Crea el objeto DAG
dag = DAG(
    dag_id="ingesta_dag",
    default_args=default_args,
    catchup=False  # Evita la ejecución retroactiva de tareas
)

# Define las tareas del DAG
with dag:
    run_script_ingest = BashOperator(
        task_id='run_script_Ingesta',
        bash_command='python /user/app/ProyectoEndToEndPython/Proyecto/ingesta.py'
    )

    run_script_transform = BashOperator(
        task_id='run_script_Transformacion',
        bash_command='python /user/app/ProyectoEndToEndPython/Proyecto/Transformacion.py'
    )

    # Define la secuencia de tareas
    run_script_ingest >> run_script_transform
