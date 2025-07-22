from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

@dag(
    dag_id="google_sheets_creation_test",
    start_date=pendulum.datetime(2025, 7, 22, tz="Europe/London"),
    schedule=None,
    catchup=False,
    tags=["debug", "google"],
)
def google_sheets_test_dag():
    """A simple DAG to test creating a Google Sheet."""

    @task
    def create_a_single_sheet():
        """
        Tests the Google Sheets connection by creating a new spreadsheet.
        """
        try:
            print("Attempting to create a Google Sheet...")
            
            # 1. Instantiate the hook, pointing to your GCP connection ID
            hook = GSheetsHook(gcp_conn_id="teg_google_workspace_sheets_sa")

            # 2. Define the properties for the new spreadsheet
            spreadsheet_properties = {
                "properties": {"title": "Airflow Test Sheet - Success!"}
            }
            
            # 3. Call the create_spreadsheet method
            response = hook.create_spreadsheet(spreadsheet=spreadsheet_properties)
            
            # 4. Log the success and the new sheet's URL
            sheet_id = response['spreadsheetId']
            print("✅ Success! Google Sheet created.")
            print(f"   ID: {sheet_id}")
            print(f"   URL: https://docs.google.com/spreadsheets/d/{sheet_id}")
            
        except Exception as e:
            print("❌ ERROR: Failed to create Google Sheet.")
            print(f"   Reason: {e}")
            # Re-raise the exception to make the task fail
            raise
    
    create_a_single_sheet()

google_sheets_test_dag()
