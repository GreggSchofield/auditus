# 1. Make sure you have the right imports
from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook  # Import the HttpHook


# 2. Use the @dag decorator with required arguments
@dag(
    dag_id="my_first_dag",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "cloudflare"],
)
def my_simple_dag():
    """
    A simple example DAG that now calls the Cloudflare API.
    """

    # We've renamed the task to be more descriptive
    @task
    def get_cloudflare_zones():
        """
        This task uses the HttpHook to call the Cloudflare API
        and print the names of the zones found.
        """
        # Instantiate the HttpHook, pointing to the connection you created
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')

        # Define your query parameters as a dictionary
        query_params = {
            "per_page": 50,
            "status": "active"
        }

        # Execute the request to the /zones endpoint
        print("Calling Cloudflare API to get zones...")
        response = hook.run(endpoint='/client/v4/zones', data=query_params)

        # Process the JSON response
        data = response.json()

        # Log the results
        zone_names = [zone['name'] for zone in data['result']]
        print(f"Successfully found {len(zone_names)} zones.")
        print(f"Zone Names: {zone_names}")

        return zone_names

    # Call the task function to add it to the DAG
    get_cloudflare_zones()


# 3. CRITICAL: You must call the function at the end to create the DAG object.
my_simple_dag()
