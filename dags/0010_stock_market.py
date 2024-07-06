"""Hello world tutorial module."""

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from astro import sql as asql
from astro.files import File
from astro.sql.table import (
    Table,
    Metadata,
)
from datetime import datetime
import sys

import requests

from include.stock_market.tasks import (
    MINIO_BUCKET_NAME,
    _get_stock_prices,
    _store_prices,
    _get_formatted_csv,
)


SYMBOL: str = 'AAPL'

## --- USAGE: ---
# 1. To activate this DAG, it's necessary to rename the file ._docker-compose.override.yml
#    to docker-compose.override.yml, to enable all the docker components that the DAG will
#    need in order to run.
# 2. Import the connections from the .env file using command:
#    ``astro dev object import --connections .env``
## ---
@dag(
    start_date=datetime.today(),
    schedule='@daily',
    catchup=False,
    tags=['stock_market', SYMBOL, 'taskflow'],
    on_success_callback=SlackNotifier(
        slack_conn_id='slack_notify',
        text="The DAG ``run_stock_market`` has succeeded.",
        channel="udemy",
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack_notify',
        text="The DAG ``run_stock_market`` has failed.",
        channel="udemy",
    ),
    max_active_runs=1, # Control the number of parallel exception that the DAG can run.
)
def dag0010_run_stock_market():
    @task
    def take_py_version():
        return sys.version

    @task
    def show_py_version(value):
        print("Python version:", value)

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def check_api_availability() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json().get('finance', {}).get('result') is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        # The Jinja quote is named "Templating" in the context of Airflow,
        # meaining that it's possible to inject data into the task at runtime.
        op_kwargs={
            "url": "{{ task_instance.xcom_pull(task_ids='check_api_availability') }}",
            "symbol": SYMBOL,
        },
    )

    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={
            "stock": "{{ task_instance.xcom_pull(task_ids='get_stock_prices') }}"
        },
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image='airflow/dkr-stock-spark-app',
        container_name='dkr_format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": "{{ task_instance.xcom_pull(task_ids='store_prices') }}",
        },
    )

    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={
            "path": "{{ task_instance.xcom_pull(task_ids='store_prices') }}",
        }
    )

    load_to_dwh = asql.load_file(
        task_id="load_to_dwh",
        input_file=File(
            path=f"s3://{MINIO_BUCKET_NAME}/" + "{{ task_instance.xcom_pull(task_ids='get_formatted_csv') }}",
            conn_id="minio",
        ),
        output_table=Table(
            name="stock_market",
            conn_id="postgres",
            metadata=Metadata(
                schema="public"
            )
        )
    )

    (
        show_py_version(take_py_version()) 
        >> check_api_availability() 
        >> get_stock_prices
        >> store_prices
        >> format_prices
        >> get_formatted_csv
        >> load_to_dwh
    )


dag0010_run_stock_market()
