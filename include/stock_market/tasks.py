from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO, StringIO
import pandas as pd
from airflow.exceptions import AirflowNotFoundException

bucket_name = 'stock-market'

def get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )
    return client

def _get_stock_prices(base_url, symbol):
    import requests
    import json

    url = f"{base_url}{symbol}?region=US&lang=en-US&includePrePost=false&interval=2m&range=60d"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock_data):
    client = get_minio_client()

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



def _format_prices_pandas_from_minio(bucket_prefix: str) -> str:
    # Parse bucket + prefix (symbol)
    bucket_name, symbol = bucket_prefix.split("/", 1)
    input_key = f"{symbol}/prices.json"
    output_key = f"{symbol}/formatted_prices/formatted_prices.csv"

    client = get_minio_client()

    # ---- READ JSON from MinIO ----
    resp = client.get_object(bucket_name, input_key)
    try:
        raw_bytes = resp.read()
    finally:
        resp.close()
        resp.release_conn()

    raw = json.loads(raw_bytes.decode("utf-8"))

    timestamps = raw.get("timestamp", [])
    quotes = raw.get("indicators", {}).get("quote", [])

    if not timestamps or not quotes:
        raise ValueError(
            "Unexpected JSON shape. Expected keys: 'timestamp' and 'indicators.quote'."
        )

    q0 = quotes[0]  # usually a single element list

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": q0.get("close", []),
            "high": q0.get("high", []),
            "low": q0.get("low", []),
            "open": q0.get("open", []),
            "volume": q0.get("volume", []),
        }
    )


    df["date"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.date
    df = df.dropna(subset=["open", "high", "low", "close"], how="all").reset_index(drop=True)


    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    csv_bytes = csv_buf.getvalue().encode("utf-8")

    client.put_object(
        bucket_name=bucket_name,
        object_name=output_key,
        data=BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )

    return f"{bucket_name}/{output_key}"


def _get_formatted_csv(path):
    client = get_minio_client()
    bucket_name = 'stock-market'
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    print(prefix_name)

    objects = client.list_objects(bucket_name, prefix = prefix_name, recursive = True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException('The csv file does not exist')



