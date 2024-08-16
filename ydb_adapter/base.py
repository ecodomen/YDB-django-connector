from django.db.backends.base.base import BaseDatabaseWrapper
import ydb

from ydb_adapter.client import DatabaseClient
from ydb_adapter.creation import DatabaseCreation
from ydb_adapter.features import DatabaseFeatures
from ydb_adapter.introspection import DatabaseIntrospection
from ydb_adapter.schema_editor import DatabaseSchemaEditor


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'ydb'
    display_name = 'YDB'

    # Define classes to use within the wrapper
    Database = ydb
    features_class = DatabaseFeatures
    creation_class = DatabaseCreation
    introspection_class = DatabaseIntrospection
    schema_editor_class = DatabaseSchemaEditor
    client_class = DatabaseClient
    # TODO: Implement the remaining necessary classes
    # operations_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = None

    def get_connection_params(self) -> dict:
        """Retrieve connection parameters from settings."""
        settings_dict = self.settings_dict
        return {
            "endpoint": settings_dict['HOST'],
            "database": settings_dict['NAME']
        }

    def get_new_connection(self, conn_params: dict):
        """Create a new connection to YDB."""
        return ydb.Driver(endpoint=conn_params['endpoint'], database=conn_params['database'])

    def init_connection_state(self):
        """Initialize connection state."""
        if not self.connection.is_connected:
            self.connection.connect()

    def create_cursor(self):
        """Create a cursor for executing queries."""
        return self.connection.cursor()

    def is_usable(self) -> bool:
        """Check if the connection is usable."""
        try:
            self.connection.ping()
            return True
        except ydb.Error:
            return False

    def close(self):
        """Close the connection to the database."""
        if self.connection is not None:
            self.connection.close()
