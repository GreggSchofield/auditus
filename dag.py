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
    
    @task(pool="cloudflare_api_pool")
    def check_argo_routing(zone: dict) -> dict:
        """
        Takes a single zone and checks its main Argo Smart Routing status.
        """
        zone_id = zone['id']
        zone_name = zone['name']
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        # This endpoint checks the main Argo Smart Routing setting
        endpoint = f"/client/v4/zones/{zone_id}/argo"
        
        response = hook.run(endpoint=endpoint)
        response.raise_for_status()
        
        argo_status = response.json()['result']['value']
        
        return {"zone_name": zone_name, "argo_status": argo_status}
    
    @task
    def summarize_argo_status(results: list[dict]):
        """
        Summarizes the audit and prints a list of non-compliant zones.
        """
        print("--- Argo Routing Audit Summary ---")
        
        # 1. Create a new list containing only the names of zones where Argo is 'off'.
        disabled_zones = [
            result['zone_name'] for result in results if result['argo_status'] == 'off'
        ]
        
        # 2. Check if the list has any domains in it.
        if disabled_zones:
            print(f"Found {len(disabled_zones)} zones with Argo Routing DISABLED:")
            # 3. Print each domain on a new line.
            for zone_name in disabled_zones:
                print(f"- {zone_name}")
        else:
            print("✅ All zones have Argo Routing enabled. No action needed.")
            
        print("--- End of Summary ---")

    # Map-Reduce Cloudflare Zones
    page_numbers = get_total_pages()
    zone_lists = get_one_page_of_zones.expand(page_number=page_numbers)
    all_zones = combine_results(zone_lists)

    # The list of all zones is now passed to the new argo check task
    argo_routing_results = check_argo_routing.expand(zone=all_zones)
    
    # The final task now summarizes the argo status
    summarize_argo_status(argo_routing_results)


cloudflare_paginated_dag()
