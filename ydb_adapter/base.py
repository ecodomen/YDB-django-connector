from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
import ydb


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'ydb'
    display_name = 'YDB'

    def __init__(self, settings_dict, alias='default'):
        super().__init__(settings_dict, alias)

        self.connection = None
        self.settings_dict = settings_dict

    def get_connection_params(self):
        return {
            "connection_string": self.settings_dict['NAME'],
        }

    def get_new_connection(self, conn_params):
        return ydb.Driver(endpoint=conn_params['connection_string'], database='/path/to/database')

    def init_connection_state(self):
        pass  # Здесь могут быть действия для инициализации состояния соединения

    def create_cursor(self):
        return self.connection.cursor()
