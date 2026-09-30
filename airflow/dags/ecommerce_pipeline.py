from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG('enterprise_ecommerce_pipeline', start_date=datetime(2026,1,1), schedule='@daily', catchup=False, tags=['portfolio','pyspark']) as dag:
    generate=BashOperator(task_id='generate_source_data', bash_command='cd /opt/project && python -m src.generators.generate_all')
    bronze=BashOperator(task_id='ingest_bronze', bash_command='cd /opt/project && python -m src.ingestion.local_to_bronze')
    silver=BashOperator(task_id='build_silver', bash_command='cd /opt/project && python -m src.transformations.bronze_to_silver')
    quality=BashOperator(task_id='quality_checks', bash_command='cd /opt/project && python -m src.quality.run_quality_checks')
    gold=BashOperator(task_id='build_gold', bash_command='cd /opt/project && python -m src.transformations.build_gold')
    generate >> bronze >> silver >> quality >> gold
