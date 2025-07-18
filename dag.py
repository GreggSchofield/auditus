from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

@dag(
    dag_id="cloudflare_dynamic_zone_fetch",
    start_date=pendulum.datetime(2025, 7, 18, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "cloudflare", "dynamic"],
)
def cloudflare_paginated_dag():
    """
    This DAG dynamically fetches all pages of Cloudflare zones in parallel
    using the HttpHook and Dynamic Task Mapping.
    """

    @task
    def get_total_pages() -> list[int]:
        """
        Makes one API call to find the total number of pages and returns
        a list of page numbers to be fetched.
        """
        print("--- Determining total number of pages ---")
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        
        # We only need 1 result to get the 'result_info' containing page counts
        response = hook.run(endpoint='/client/v4/zones', data={"per_page": 1})
        data = response.json()
        
        total_pages = data['result_info']['total_pages']
        print(f"Total pages to fetch: {total_pages}")
        
        # Return a list of page numbers, e.g., [1, 2, 3]
        return list(range(1, total_pages + 1))

    @task
    def get_one_page_of_zones(page_number: int) -> list[str]:
        """
        This task is dynamically mapped. It fetches a single page of zones.
        """
        print(f"--- Fetching page {page_number} of zones ---")
        hook = HttpHook(method='GET', http_conn_id='cloudflare_api_default')
        
        # Pass the page number and desired page size to the API call
        params = {"per_page": 50, "page": page_number}
        response = hook.run(endpoint='/client/v4/zones', data=params)
        response.raise_for_status()

        data = response.json()
        zone_names = [zone['name'] for zone in data['result']]
        print(f"Page {page_number}: Found {len(zone_names)} zones.")
        return zone_names
        
    @task
    def combine_results(list_of_zone_lists: list[list[str]]):
        """
        This "reduce" task takes the list of lists from the mapped tasks
        and flattens it into a single list of all zones.
        """
        print("--- Combining all results ---")
        
        all_zones = [zone for sublist in list_of_zone_lists for zone in sublist]
        
        print(f"Total zones found across all pages: {len(all_zones)}")
        print(f"Final list: {all_zones}")
        
        return all_zones

    # Define the workflow
    page_numbers = get_total_pages()
    
    # "Map" step: Creates parallel tasks based on the list of page numbers
    zone_lists = get_one_page_of_zones.expand(page_number=page_numbers)
    
    # "Reduce" step: Collects all the results into a final list
    combine_results(zone_lists)

cloudflare_paginated_dag()
