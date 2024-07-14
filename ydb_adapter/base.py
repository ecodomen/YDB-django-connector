from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
import ydb

from ydb_adapter.features import DatabaseFeatures


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'ydb'
    display_name = 'YDB'

    # Определение классов для использования в обертке
    # TODO: необходимо реализовать все остальные классы
    Database = ydb
    # schema_editor_class = DatabaseSchemaEditor
    # creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    # introspection_class = DatabaseIntrospection
    # operations_class = DatabaseOperations
    # client_class = DatabaseClient

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = None

    def get_connection_params(self):
        # Получение параметров соединения из настроек
        settings_dict = self.settings_dict
        return {
            "endpoint": settings_dict['HOST'],
            "database": settings_dict['NAME']
        }

    def get_new_connection(self, conn_params):
        # Создание нового соединения с YDB
        return ydb.Driver(endpoint=conn_params['endpoint'], database=conn_params['database'])

    def init_connection_state(self):
        # Инициализация состояния соединения
        if not self.connection.is_connected:
            self.connection.connect()

    def create_cursor(self):
        # Создание курсора для выполнения запросов
        return self.connection.cursor()

    def is_usable(self):
        # Проверка, является ли соединение используемым
        try:
            self.connection.ping()
            return True
        except ydb.Error:
            return False

    def close(self):
        # Закрытие соединения с базой данных
        if self.connection is not None:
            self.connection.close()
