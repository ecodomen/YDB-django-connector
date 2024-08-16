from django.db.backends.base.client import BaseDatabaseClient
import ydb
import os


class DatabaseClient(BaseDatabaseClient):
    def runshell(self):
        """Runs a database command line interface without needing external parameters."""
        conn_params = self.connection.get_connection_params()
        self.run_ydb_shell(conn_params)

    @staticmethod
    def run_ydb_shell(conn_params):
        """Configures and starts a YDB session for interactive or command-line usage."""
        # Ensure that you're using the correct method to fetch credentials.
        token = os.getenv("YDB_TOKEN")
        credentials = ydb.iam.TokenCredentials(token)  # Assuming correct import path

        driver_config = ydb.DriverConfig(
            endpoint=conn_params['endpoint'],
            database=conn_params['database'],
            credentials=credentials
        )

        # Create a driver instance to manage connections
        with ydb.Driver(driver_config) as driver:
            driver.wait(fail_fast=True, timeout=5)  # Ensure the driver is ready to accept requests

            # Establish a session to execute queries
            with driver.table_client.session().create() as session:
                print("YDB interactive shell is ready. Type your SQL commands.")
                while True:
                    try:
                        command = input("sql> ")
                        if command.lower() == 'exit':
                            break
                        result = session.transaction().execute(
                            command,
                            commit_tx=True,
                            settings=ydb.BaseRequestSettings().with_timeout(3)
                        )
                        for row in result.result_sets[0].rows:
                            print(row)
                    except Exception as e:
                        print(f"An error occurred: {e}")

    @staticmethod
    def close_connection():
        """Placeholder method to demonstrate how to close a YDB connection if needed."""
        print("Connection management is typically handled by the YDB driver.")
