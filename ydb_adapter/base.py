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

    # Определение классов для использования в обертке
    # TODO: необходимо реализовать все остальные классы
    Database = ydb
    # SchemaEditorClass = DatabaseSchemaEditor
    # CreationClass = DatabaseCreation
    # FeaturesClass = DatabaseFeatures
    # IntrospectionClass = DatabaseIntrospection
    # OperationsClass = DatabaseOperations
    # ClientClass = DatabaseClient

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