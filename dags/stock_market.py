from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices
from include.stock_market.tasks import _store_prices


SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']

)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'base_url': '{{ ti.xcom_pull(task_ids="is_api_available", key="return_value") }}',
            'symbol': SYMBOL
        }
    )

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable = _store_prices,
        op_kwargs = {'stock_data' : '{{ ti.xcom_pull(task_ids="get_stock_prices", key="return_value") }}'  }
    )

    # is_api_available()
    # cmd to test the task
    # astro dev run tasks test stock_market is_api_available

    # is_api_available() >> get_stock_prices
    # # cmd to test - astro dev run dags test stock_market
    # # we cannot run just the get_stock_prices task because it is dependent on is_api_available task

    is_api_available() >> get_stock_prices >> store_prices

stock_market()