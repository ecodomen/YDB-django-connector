from django.db.backends.base.creation import BaseDatabaseCreation
from django.core.exceptions import ImproperlyConfigured


class DatabaseCreation(BaseDatabaseCreation):
    @staticmethod
    def _quote_name(name: str) -> str:
        """
        Quotes a database identifier for Yandex Database (YDB) to ensure it is interpreted correctly.
        Args:
            name (str): The identifier to quote.
        Returns:
            str: The quoted identifier.
        """
        return f'"{name}"'

    @staticmethod
    def _get_database_create_suffix() -> str:
        """
        Returns an empty string as YDB does not utilize encoding or template in SQL commands for database creation.
        Returns:
            str: An empty string, indicating no suffix is needed for database creation in YDB.
        """
        return ""

    def sql_table_creation_suffix(self) -> str:
        """
        Returns the SQL suffix for creating a table in YDB, noting the database does not support COLLATION or CHARSET
        settings.
        Raises:
            ImproperlyConfigured: If the COLLATION setting is set, as it is not supported by YDB.
        Returns:
            str: An appropriate SQL suffix for table creation.
        """
        test_settings = self.connection.settings_dict["TEST"]
        if test_settings.get("COLLATION") is not None:
            raise ImproperlyConfigured("YDB does not support collation setting at database creation time.")
        return ""

    @staticmethod
    def _database_exists(cursor, database_name: str) -> bool:
        """
        Checks if a database exists in YDB.
        Args:
            cursor: The database cursor.
            database_name (str): The name of the database to check.
        Returns:
            bool: True if the database exists, False otherwise.
        """
        cursor.execute("SELECT 1 FROM system.tables WHERE database = %s", [database_name])
        return cursor.fetchone() is not None

    def _execute_create_test_db(self, cursor, parameters: dict, keepdb: bool = False) -> None:
        """
        Executes the SQL command to create a test database in YDB, handling existing database checks.
        Args:
            cursor: The database cursor.
            parameters (dict): Parameters including the database name.
            keepdb (bool): Whether to keep the database if it already exists.
        """
        if keepdb and self._database_exists(cursor, parameters["dbname"]):
            return
        cursor.execute(f"CREATE DATABASE {self._quote_name(parameters['dbname'])}")

    def _destroy_test_db(self, test_database_name: str, verbosity: int) -> None:
        """
        Destroys a test database in YDB.
        Args:
            test_database_name (str): The name of the test database to be deleted.
            verbosity (int): The level of verbosity.
        """
        print(f"Deleting test database '{test_database_name}'...")
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE {self._quote_name(test_database_name)}")

    def destroy_test_db(self, old_database_name: str, verbosity: int = 1) -> None:
        """
        Destroys the test database and restores the original database, if necessary, in YDB.
        Args:
            old_database_name (str): The name of the old database to destroy.
            verbosity (int): The level of verbosity for the operation.
        """
        self._destroy_test_db(old_database_name, verbosity)
        print("Test database deleted.")
