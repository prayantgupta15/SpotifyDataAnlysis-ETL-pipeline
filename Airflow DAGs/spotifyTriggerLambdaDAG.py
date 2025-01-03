from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

with DAG(
    dag_id='SpotifyETL',
    schedule_interval= '0 7 * * *', #Runs at 7AM UTC (12:30PM IST) daily
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=2),
    tags=['Spotify','ETL','Lambda'],
    catchup=False,
) as dag:
    # [START howto_operator_lambda]
    invoke_lambda_function = LambdaInvokeFunctionOperator(
        task_id='extractData',
        function_name='testLaambda'
    )
    
