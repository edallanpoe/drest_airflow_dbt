
from io import BytesIO
import json
import os
import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from minio import Minio


MINIO_BUCKET_NAME = "stock-market"


def _get_minio_conn() -> Minio:
    api = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=api.extra_dejson["endpoint_url"].split("//")[-1],
        access_key=api.login,
        secret_key=api.password,
        secure=False,
    )
    if not client.bucket_exists(MINIO_BUCKET_NAME):
        client.make_bucket(MINIO_BUCKET_NAME)

    return client


def _get_stock_prices(url, symbol) -> str:
    api = BaseHook.get_connection('stock_api')
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock) -> os.PathLike:
    client = _get_minio_conn()
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode("utf8")
    obj = client.put_object(
        MINIO_BUCKET_NAME, f"{symbol}/prices.json", data=BytesIO(data), length=len(data)
    )
    return f"{obj.bucket_name}/{symbol}"


def _get_formatted_csv(path):
    client = _get_minio_conn()
    prefix = f"{path.split('/')[1]}/formatted_prices/"
    objs = client.list_objects(
        MINIO_BUCKET_NAME, prefix=prefix, recursive=True
    )
    for obj in objs:
        if obj.object_name.endswith('.csv'):
            return obj.object_name

    raise AirflowNotFoundException(f"CSV file not doun in prefix {prefix}")