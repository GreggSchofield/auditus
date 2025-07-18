from __future__ import annotations

import pendulum
import math
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.models.variable import Variable

@dag(
    dag_id="cloudflare_dynamic_zone_fetch",
    start_date=pendulum.datetime(2025, 7, 18, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["cloudflare", "dynamic"],
)
def cloudflare_paginated_dag():
    
    @task
    def get_total_pages() -> list[int]:
        """
        Makes one API call to find the total number of zones and correctly
        calculates the number of pages needed.
        """
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        account_id = Variable.get("cloudflare_account_id")
        params = {"per_page": 1, "account.id": account_id}
        
        response = hook.run(endpoint='/client/v4/zones', data=params)
        data = response.json()
        
        total_zones = data['result_info']['total_count']
        per_page = 50
        
        total_pages_calculated = math.ceil(total_zones / per_page)
        
        print(f"Account {account_id} has {total_zones} zones. Calculated pages: {total_pages_calculated}")
        
        return list(range(1, total_pages_calculated + 1))

    @task
    def get_one_page_of_zones(page_number: int) -> list[str]:
        """
        Fetches a single page of zones and returns only their names.
        """
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        account_id = Variable.get("cloudflare_account_id")
        params = {"per_page": 50, "page": page_number, "account.id": account_id}
        
        response = hook.run(endpoint='/client/v4/zones', data=params)
        response.raise_for_status()
        
        data = response.json()
        
        zone_names = [zone['name'] for zone in data['result']]
        
        return zone_names
        
    @task
    def combine_results(list_of_zone_lists: list[list[dict]]):
        all_zones = [zone for sublist in list_of_zone_lists for zone in sublist]
        print(f"Total zones found for this account: {len(all_zones)}")
        return all_zones

    page_numbers = get_total_pages()
    zone_lists = get_one_page_of_zones.expand(page_number=page_numbers)
    combine_results(zone_lists)

cloudflare_paginated_dag()