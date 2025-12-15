from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random
 
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
)
def random_number_checker():
    @task
    def generate_random_number():
        # ti = context['ti']
        number = random.randint(1, 100)
        # ti.xcom_push(key='random_number', value=number)
        print(f"Generated random number: {number}")
        return number
    @task
    def check_even_odd(num):
        # ti = context['ti']
        # number = ti.xcom_pull(task_ids='generate_number', key='random_number')

        result = "even" if num % 2 == 0 else "odd"
        print(f"The number {num} is {result}.")
    
    check_even_odd(generate_random_number())

random_number_checker()