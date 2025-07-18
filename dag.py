from __future__ import annotations
import pendulum
import math
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

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
        Makes 1 API call to find the total number of zones and correctly
        calculates the number of pages needed.
        """
        print("--- Determining total number of pages ---")
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        
        # Fetch with per_page=1 to be efficient
        response = hook.run(endpoint='/client/v4/zones', data={"per_page": 1})
        data = response.json()
        
        # The API provides total_count, not total_pages
        total_zones = data['result_info']['total_count']
        per_page = 50 # The page size we'll use for fetching
        
        # Correctly calculate the number of pages using math.ceil()
        total_pages_calculated = math.ceil(total_zones / per_page)
        
        print(f"Total zones: {total_zones}. Calculated pages to fetch: {total_pages_calculated}")
        
        # Return a list of page numbers, e.g., for 146 zones this will be [1, 2, 3]
        return list(range(1, total_pages_calculated + 1))

    @task
    def get_one_page_of_zones(page_number: int) -> list[str]:
        """
        Fetches a single page of 50 zones. This task will be
        cloned N times, where N is the number of pages.
        """
        print(f"--- Fetching page {page_number} of zones ---")
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        params = {"per_page": 50, "page": page_number}
        response = hook.run(endpoint='/client/v4/zones', data=params)
        response.raise_for_status()

        data = response.json()
        # This task returns a list of up to 50 zone names
        zone_names = [zone['name'] for zone in data['result']]
        return zone_names
        
    @task
    def combine_results(list_of_zone_lists: list[list[str]]):
        """
        Combines the lists of zones from each page fetch.
        """
        print("--- Combining all results ---")
        all_zones = [zone for sublist in list_of_zone_lists for zone in sublist]
        print(f"Total zones found across all pages: {len(all_zones)}")
        return all_zones

    # This is the crucial part of the workflow
    page_numbers = get_total_pages()
    
    # .expand() is called on the SHORT list of page numbers ([1, 2, 3])
    # This creates exactly 3 parallel tasks.
    zone_lists = get_one_page_of_zones.expand(page_number=page_numbers)
    
    # The final task just combines the results from those 3 tasks.
    combine_results(zone_lists)

cloudflare_paginated_dag()
