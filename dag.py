from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.models.variable import Variable # 1. Import the Variable class

@dag(
    dag_id="my_first_dag",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "cloudflare"],
)
def my_simple_dag():
    @task
    def get_cloudflare_zones():
        """
        This task securely gets a token from Airflow Variables
        and uses it to call the Cloudflare API.
        """
        # 2. Securely get the token's value from the variable
        auth_token = Variable.get("cloudflare_api_token")
        
        # 3. Construct the header dictionary
        custom_headers = {"Authorization": f"Bearer {auth_token}"}

        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        
        # 4. Pass the secure headers to the run() method
        response = hook.run(endpoint='/client/v4/zones', headers=custom_headers)

        data = response.json()
        zone_names = [zone['name'] for zone in data['result']]
        print(f"Successfully found {len(zone_names)} zones.")
        return zone_names

    get_cloudflare_zones()

my_simple_dag()