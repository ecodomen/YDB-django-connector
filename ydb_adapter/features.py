from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    uses_savepoints = True
    supports_transactions = True
    can_return_rows_from_bulk_insert = True
    has_select_for_update = True
    has_select_for_update_nowait = False
    supports_tablespaces = True
    supports_paramstyle_pyformat = True
    supports_sequence_reset = False
    supports_json_field = True
    supports_index_on_text_field = True
    supports_atomic_references_rename = False
    can_introspect_foreign_keys = True
    supports_timezones = False
    requires_literal_defaults = False
    can_clone_databases = True
    supports_temporal_subtraction = True

    def supports_transactions(self):
        # Пример проверки версии базы данных для определения поддержки транзакций
        return self.connection.server_version >= (2, 0)
