from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

@dag(
    dag_id="debug_connection_dag",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["debug"],
)
def debug_connection_dag():
    @task
    def inspect_connection_extras():
        """
        Fetches a connection and prints its Extras field to the logs
        to verify its exact structure and content.
        """
        print("--- Getting connection 'cloudflare_api_default' ---")
        conn = BaseHook.get_connection("cloudflare_api_default")

        print("\n1. Raw 'conn.extra' (the exact string from the database):")
        # repr() is used to make hidden characters like \n or \t visible
        print(repr(conn.extra))

        print("\n2. Parsed 'conn.extra_dejson' (the Python dictionary Airflow uses):")
        print(conn.extra_dejson)

        # Specifically check the headers part
        if 'headers' in conn.extra_dejson:
            print("\n3. The 'headers' object inside the dictionary:")
            print(conn.extra_dejson['headers'])
        else:
            print("\n3. The 'headers' key was NOT found in the Extras dictionary.")

    inspect_connection_extras()

debug_connection_dag()