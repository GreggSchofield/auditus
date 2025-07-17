# 1. Make sure you have the right imports
from __future__ import annotations
import pendulum
from airflow.decorators import dag, task

# 2. Use the @dag decorator with required arguments
@dag(
    dag_id="my_first_dag",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
)
def my_simple_dag():
    """A simple example DAG."""

    @task
    def my_first_task():
        print("Hello, Airflow!")

    my_first_task()

# 3. CRITICAL: You must call the function at the end to create the DAG object.
my_simple_dag()
