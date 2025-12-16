from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

format_prices = SparkSubmitOperator(
    task_id="format_prices",
    application="/usr/local/airflow/include/spark_jobs/format_prices.py",
    conn_id="spark_default",
    verbose=True,

    # If you don't have a spark connection created in Airflow UI,
    # you can pass master directly using conf:
    conf={
        "spark.master": "spark://spark-master:7077",
    },

    # templated args: this will pull the same XCom you used before
    application_args=[
        "{{ ti.xcom_pull(task_ids='store_prices', key='return_value') }}"
    ],
)
