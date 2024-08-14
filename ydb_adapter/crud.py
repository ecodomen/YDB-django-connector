from django.db.backends.base.creation import BaseDatabaseCreation
from typing import Optional
from django.db import models


class DatabaseCreation(BaseDatabaseCreation):
    def create_test_db(self, verbosity: int = 1, autoclobber: bool = False, serialize: bool = True) -> str:
        """
        Creates a test database. If the database already exists and autoclobber is False,
        Django will prompt the user for confirmation to delete the database.
        """
        test_database_name = self._get_test_db_name()

        if self.connection.introspection.database_exists(test_database_name) and not autoclobber:
            # Prompt for confirmation to delete the existing test database
            confirm = input(
                f"Test database '{test_database_name}' already exists. Delete it? [yes/no] ")
            if confirm != 'yes':
                print("Testing cancelled.")
                return test_database_name

        # Destroy the old test database and create a new one
        self._destroy_test_db(test_database_name, verbosity)
        self._create_test_db(verbosity, autoclobber, serialize)

        return test_database_name

    def _create_test_db(self, verbosity: int, autoclobber: bool, serialize: bool) -> None:
        """
        Internal method to create a test database.
        """
        print("Creating test database...")
        # Logic to create the database in YDB goes here

    def _destroy_test_db(self, test_database_name: str, verbosity: int) -> None:
        """
        Destroys the test database.
        """
        print(f"Deleting test database '{test_database_name}'...")
        # Logic to delete the test database in YDB goes here

    def destroy_test_db(self, old_database_name: str, verbosity: int = 1) -> None:
        """
        Destroys the test database and restores the original database, if necessary.
        """
        self._destroy_test_db(old_database_name, verbosity)
        print("Test database deleted.")
