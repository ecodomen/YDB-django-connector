from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db import models
from typing import Optional, Type


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    def create_model(self, model: Type[models.Model]) -> None:
        """Creates a new database table for the given model."""
        columns = [
            f"{field.column} {self.data_type(field)}" for field in model._meta.fields
        ]
        sql = f"CREATE TABLE {model._meta.db_table} ({', '.join(columns)});"
        self.execute(sql)

    def delete_model(self, model: Type[models.Model]) -> None:
        """Deletes the model's table from the database."""
        sql = f"DROP TABLE {model._meta.db_table};"
        self.execute(sql)

    def add_field(self, model: Type[models.Model], field: models.Field) -> None:
        """Adds a new field to the model's table."""
        sql = f"ALTER TABLE {model._meta.db_table} ADD COLUMN {field.column} {self.data_type(field)};"
        self.execute(sql)

    def remove_field(self, model: Type[models.Model], field: models.Field) -> None:
        """Removes a field from the model's table."""
        sql = f"ALTER TABLE {model._meta.db_table} DROP COLUMN {field.column};"
        self.execute(sql)

    def alter_field(self, model: Type[models.Model], old_field: models.Field,
                    new_field: models.Field, strict: bool = False) -> None:
        """Changes an existing field in the model's table."""
        # This assumes changing the data type for simplicity, more logic needed for full support
        sql = (
            f"ALTER TABLE {model._meta.db_table} "
            f"ALTER COLUMN {old_field.column} "
            f"SET DATA TYPE {self.data_type(new_field)};"
        )
        self.execute(sql)

    def execute(self, sql: str, params: Optional[dict] = None) -> None:
        """Executes an SQL command against the database."""
        print(f"Executing SQL: {sql}")
        # The actual execution code using the YDB connection should go here

    @staticmethod
    def data_type(field: models.Field) -> str:
        """Returns the SQL string for a field's data type based on Django field types."""
        if isinstance(field, models.CharField):
            return 'VARCHAR(255)'
        elif isinstance(field, models.IntegerField):
            return 'INTEGER'
        elif isinstance(field, models.BooleanField):
            return 'BOOLEAN'
        # Extend this to include other Django field types as needed
