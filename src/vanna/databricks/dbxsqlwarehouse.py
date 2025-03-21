"""
This manages a connection to a Databricks SQL endpoint,
and via the warehouse, a query interface for SQL.
"""

import databricks.sql
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from ..base import VannaBase
from ..exceptions import DependencyError
class DBXSQLWarehouse(VannaBase):
    def __init__(self, server_hostname, http_path, access_token):
        try:
            import databricks.sql
        except ImportError:
            raise DependencyError(
                "You need to install required dependencies to execute this method, run command:"
                " \npip install databricks-sql-connector"
            )
        
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.request_headers = {"Authorization": f"Bearer {self.access_token}"}
        self.warehouse_id = self.http_path.split("/")[-1]
        self.warehouse_status = self.get_warehouse_status()
        self.retry_config = {
            "warehouse_retries": 5, 
            "warehouse_retry_delay": 15, 
            "warehouse_retry_backoff": 2,
            "warehouse_max_rety_time": 60
        }
        self._start_warehouse() if not self.warehouse_status in ["RUNNING","STARTING"] else None

    def get_retry_decorator(self):
        """Return a retry decorator configured with instance-specific settings."""
        return retry(
            stop=stop_after_attempt(self.retry_config["warehouse_retries"]),
            wait=wait_exponential(
                multiplier=1,
                min=self.retry_config["warehouse_retry_delay"],
                max=self.retry_config["warehouse_max_rety_time"],
            ),
        )
        

    def get_warehouse_status(self):
        url = f"https://{self.server_hostname}/api/2.0/sql/warehouses/{self.warehouse_id}"
        response = requests.get(url, headers=self.request_headers)

        if response.status_code == 200:
            self.warehouse_status = response.json().get("state","UNKNOWN")
            print(f"🔍 SQL Warehouse Status: {self.warehouse_status}")
            return self.warehouse_status
        else:
            print(f"⚠️ Error checking warehouse status: {response.text}")
            raise Exception(f"Error checking warehouse status: {response.text}")


    
    def retry_warehouse(self):
        """While the warehouse status is 'STARTING', retry the warehouse start."""
        @self.get_retry_decorator()
        def _retry_logic():
            status = self.get_warehouse_status()
            if status == "STARTING":
                print("⏳ Warehouse is still starting. Retrying...")
                self.warehouse_status = "STARTING"
                raise Exception("Warehouse still starting.")
            elif status == "RUNNING":
                print("✅ Warehouse is now running.")
                self.warehouse_status = "RUNNING"
                return
            else:
                print(f"⚠️ Unexpected warehouse status: {status}. Retrying...")
                raise Exception(f"Unexpected warehouse status: {status}")
        _retry_logic()

    def _start_warehouse(self):
        """Start the SQL Warehouse if it is stopped."""
        if self.warehouse_status != "RUNNING":
            print("🚀 Starting SQL Warehouse...")
            start_url = f"https://{self.server_hostname}/api/2.0/sql/warehouses/{self.warehouse_id}/start"
            response = requests.post(start_url, headers=self.request_headers)

            if response.status_code == 200:
                print("✅ Databricks SQL Warehouse start request sent. Waiting for it to be ready...")
                self.retry_warehouse()  # Ensure it's up before proceeding
            else:
                print(f"⚠️ Error starting warehouse: {response.text}")

    def ensure_warehouse_running(self):
        """Ensure the warehouse is running before executing queries. If its status is 'STARTING', wait."""
        if self.warehouse_status != "RUNNING":
            print("Warehouse is not running. Starting or retrying...")
            self._start_warehouse()

    def run_dbx_sql(self, sql):
        """Execute SQL against Databricks SQL Warehouse and return results as a Pandas DataFrame."""
        with databricks.sql.connect(
            self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        ) as conn:
            with conn.cursor() as cursor:
                print(f"🎯 Executing SQL: {sql}")
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)

# Usage Example
if __name__ == "__main__":
    import os
    print("Running DBX SQL Warehouse Manager...")
    # Load environment variables
    server_hostname = os.getenv("DBX_SERVER_HOSTNAME")
    http_path = os.getenv("DBX_SQLW_PATH")
    access_token = os.getenv("DBX_ACCESS_TOKEN")
    print("Variables loaded.")
    # Initialize warehouse manager
    print("Initializing DBX SQL Warehouse Manager...")
    warehouse = DBXSQLWarehouse(server_hostname, http_path, access_token)
    print(f"DBX SQL Warehouse Manager initialized for Warehouse ID: {warehouse.warehouse_id}")
    # Run a test SQL query
    df = warehouse.run_dbx_sql("SELECT 1=1;")
    print(df)