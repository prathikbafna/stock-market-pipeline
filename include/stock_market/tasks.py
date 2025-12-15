from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import json


def _get_stock_prices(base_url, symbol):
    import requests
    import json

    url = f"{base_url}{symbol}?region=US&lang=en-US&includePrePost=false&interval=2m&range=60d"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])



def _store_prices(stock_data):
    minio = BaseHook.get_connection('minio')
    print("testing minio connection")
    print(minio.extra_dejson['endpoint_url'].split('//')[1])
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )

    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    

    stock = json.loads(stock_data)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii = False).encode('utf8')
    objw = client.put_object(
        bucket_name = bucket_name,
        object_name = f'{symbol}/prices.json',
        data = BytesIO(data),
        length = len(data)
    )

    return f'{objw.bucket_name}/{symbol}'

