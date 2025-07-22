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
    def get_one_page_of_zones(page_number: int) -> list[dict]: # Return type is list[dict] again
        """
        Fetches a single page of zones for a specific account.
        """
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        account_id = Variable.get("cloudflare_account_id")
        params = {"per_page": 50, "page": page_number, "account.id": account_id}
        
        response = hook.run(endpoint='/client/v4/zones', data=params)
        response.raise_for_status()
        
        return response.json()['result']
        
    @task
    def combine_results(list_of_zone_lists: list[list[dict]]):
        all_zones = [zone for sublist in list_of_zone_lists for zone in sublist]
        print(f"Total zones found for this account: {len(all_zones)}")
        return all_zones
    
    @task(pool="cloudflare_api_pool")
    def check_tiered_caching(zone: dict) -> dict:
        """
        Takes a single zone dictionary, checks its tiered caching status,
        and returns the result.
        """
        zone_id = zone['id']
        zone_name = zone['name']
        
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        endpoint = f"/client/v4/zones/{zone_id}/argo/tiered_caching"
        
        response = hook.run(endpoint=endpoint)
        response.raise_for_status()
        
        data = response.json()
        
        caching_status = data['result']['value']
        
        print(f"Zone: '{zone_name}' -> Tiered Caching is '{caching_status}'")
        
        return {
            "zone_name": zone_name,
            "tiered_caching_status": caching_status
        }
    
    @task
    def log_final_summary(results: list[dict]):
        """This is the final 'reduce' step to see all the results."""
        print("--- Tiered Caching Audit Summary ---")
        # You could add logic here to write to a Google Sheet
        on_count = sum(1 for r in results if r['tiered_caching_status'] == 'on')
        off_count = len(results) - on_count
        print(f"Total zones checked: {len(results)}")
        print(f"Zones with Tiered Caching ON: {on_count}")
        print(f"Zones with Tiered Caching OFF: {off_count}")
        print("--- End of Summary ---")

    # Map-Reduce Cloudflare Zones
    page_numbers = get_total_pages()
    zone_lists = get_one_page_of_zones.expand(page_number=page_numbers)
    all_zones = combine_results(zone_lists)

    # Map-Reduce Configurations per Zone
    tiered_caching_results = check_tiered_caching.expand(zone=all_zones)
    log_final_summary(tiered_caching_results)


cloudflare_paginated_dag()
