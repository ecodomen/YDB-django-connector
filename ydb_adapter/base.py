import ydb
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db import utils
from django.utils.functional import cached_property

from ydb_adapter.client import DatabaseClient
from ydb_adapter.creation import DatabaseCreation
from ydb_adapter.features import DatabaseFeatures
from ydb_adapter.introspection import DatabaseIntrospection
from ydb_adapter.schema_editor import DatabaseSchemaEditor
# from ydb_adapter.operations import DatabaseOperations


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'ydb'
    display_name = 'YDB'

    # Define classes to use within the wrapper
    Database = ydb
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    # ops_class = DatabaseOperations
    schema_editor_class = DatabaseSchemaEditor
    # operations_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.connection = None  # ydb.Driver instance
        self.session = None     # ydb.Session instance
        self.transaction = None  # Current transaction

        self.settings_dict = self.settings_dict.copy()
        # Set the transaction isolation level
        self.isolation_level = self.settings_dict.get('OPTIONS', {}).get('isolation_level', 'SERIALIZABLE_READ_WRITE')

    def get_connection_params(self):
        """Retrieve connection parameters from Django settings."""
        settings_dict = self.settings_dict
        conn_params = {
            'endpoint': settings_dict.get('HOST', 'localhost:2135'),
            'database': settings_dict.get('NAME', '/local'),
            # 'credentials': None,  # Add credentials if required
            # Additional parameters can be passed through OPTIONS
        }
        options = settings_dict.get('OPTIONS', {})
        conn_params.update(options)
        return conn_params

    def get_new_connection(self, conn_params):
        """Create a new YDB driver and establish a connection."""
        # Create a driver configuration
        driver_config = ydb.DriverConfig(
            endpoint=conn_params['endpoint'],
            database=conn_params['database'],
            # credentials=conn_params.get('credentials'),  # Uncomment if using credentials
        )
        # Create the driver
        driver = ydb.Driver(driver_config)
        # Wait for the driver to become ready
        driver.wait(timeout=5)
        return driver

    def init_connection_state(self):
        """Initialize the connection state and create a session."""
        if not self.connection:
            self.connection = self.get_new_connection(self.get_connection_params())
        if not self.session:
            # Create a session
            self.session = self.connection.table_client.session().create()

    def create_cursor(self, name=None):
        """Create a cursor for executing queries."""
        self.init_connection_state()
        return CursorWrapper(self.session, self)

    def is_usable(self):
        """Check if the connection is usable."""
        try:
            # Attempt to execute a simple query
            self.session.execute_scheme('SELECT 1;')
            return True
        except ydb.Error:
            return False

    def close(self):
        """Close the session and the connection to the database."""
        if self.session:
            self.session.close()
            self.session = None
        if self.connection:
            self.connection.close()
            self.connection = None

    # Transaction management methods
    def _start_transaction_under_autocommit(self):
        """Start a transaction when autocommit is enabled."""
        self.init_connection_state()
        if not self.transaction:
            tx_mode = ydb.SerializableReadWrite()
            self.transaction = self.session.transaction(tx_mode).begin()

    def _commit(self):
        """Commit the current transaction."""
        if self.transaction:
            self.transaction.commit()
            self.transaction = None

    def _rollback(self):
        """Rollback the current transaction."""
        if self.transaction:
            self.transaction.rollback()
            self.transaction = None

    @cached_property
    def wrap_database_errors(self):
        """Wrapper to convert YDB errors to Django database errors."""
        return ydb_error_handler

    # Add additional methods if necessary


# Error handling: convert YDB exceptions to Django database exceptions
def ydb_error_handler(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ydb.OperationalError as e:
            raise utils.DatabaseError(e)
        except ydb.Error as e:
            raise utils.DatabaseError(e)
    return inner


class CursorWrapper:
    def __init__(self, session, db_wrapper):
        self.session = session
        self.db_wrapper = db_wrapper
        self.transaction = db_wrapper.transaction
        self.last_query = None
        self.results = None
        self.description = None
        self.closed = False

    @ydb_error_handler
    def execute(self, sql, params=None):
        """Execute an SQL query with optional parameters."""
        if self.closed:
            raise utils.InterfaceError("Cursor is closed.")
        if params is None:
            params = {}
        self.last_query = sql
        # Start a transaction if not already started
        if not self.transaction:
            self.db_wrapper._start_transaction_under_autocommit()
            self.transaction = self.db_wrapper.transaction

        # Prepare and execute the query
        prepared_query = self.session.prepare(sql)
        self.results = self.transaction.execute(
            prepared_query,
            params,
            commit_tx=False  # Manage the transaction manually
        )
        # Set the description of the result
        self.description = self._get_description()

    def _get_description(self):
        """Generate the description of the result columns."""
        if not self.results or not self.results[0].columns:
            return None
        columns = self.results[0].columns
        return [(col.name, None, None, None, None, None, None) for col in columns]

    def fetchone(self):
        """Fetch a single row from the result set."""
        if self.closed:
            raise utils.InterfaceError("Cursor is closed.")
        if self.results:
            result_set = self.results[0]
            if result_set.rows_left > 0:
                row = result_set.next()
                return row
        return None

    def fetchmany(self, size=None):
        """Fetch multiple rows from the result set."""
        if self.closed:
            raise utils.InterfaceError("Cursor is closed.")
        if self.results:
            result_set = self.results[0]
            rows = []
            for _ in range(size or 1):
                if result_set.rows_left > 0:
                    rows.append(result_set.next())
                else:
                    break
            return rows
        return []

    def fetchall(self):
        """Fetch all remaining rows from the result set."""
        if self.closed:
            raise utils.InterfaceError("Cursor is closed.")
        if self.results:
            result_set = self.results[0]
            rows = []
            while result_set.rows_left > 0:
                rows.append(result_set.next())
            return rows
        return []

    def close(self):
        """Close the cursor."""
        self.results = None
        self.description = None
        self.closed = True

    # Implement other cursor methods if necessary

