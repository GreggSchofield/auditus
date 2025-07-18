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
    def check_argo_routing(zone: dict) -> dict:
        """
        Takes a single zone and checks its Argo Smart Routing status,
        gracefully handling zones where Argo is not applicable.
        """
        zone_id = zone['id']
        zone_name = zone['name']
        argo_status = "not_applicable"  # Default status

        try:
            hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
            endpoint = f"/client/v4/zones/{zone_id}/argo"
            response = hook.run(endpoint=endpoint)
            data = response.json()

            if data.get('success'):
                # This task PRODUCES the 'argo_status' key
                argo_status = data['result']['value']
            else:
                print(f"WARNING: Zone '{zone_name}' -> Could not retrieve Argo status. API success=false.")
        
        except Exception as e:
            print(f"WARNING: Zone '{zone_name}' -> Could not retrieve Argo status. Error: {e}")

        # Ensure the returned dictionary always uses the correct key
        return {"zone_name": zone_name, "argo_status": argo_status}
    
    @task
    def summarize_argo_status(results: list[dict]):
        """
        Summarizes the audit and prints a list of non-compliant zones.
        """
        print("--- Argo Routing Audit Summary ---")
        
        # This task CONSUMES the 'argo_status' key
        disabled_zones = [
            result['zone_name'] for result in results if result.get('argo_status') == 'off'
        ]
        
        if disabled_zones:
            print(f"Found {len(disabled_zones)} zones with Argo Routing DISABLED:")
            for zone_name in disabled_zones:
                print(f"- {zone_name}")
        else:
            print("✅ All zones have Argo Routing enabled or are not applicable.")
            
        print("--- End of Summary ---")

    # Map-Reduce Cloudflare Zones
    page_numbers = get_total_pages()
    zone_lists = get_one_page_of_zones.expand(page_number=page_numbers)
    all_zones = combine_results(zone_lists)

    # Map-Reduce Configurations per Zone
    tiered_caching_results = check_argo_routing.expand(zone=all_zones)
    summarize_argo_status(tiered_caching_results)


cloudflare_paginated_dag()
