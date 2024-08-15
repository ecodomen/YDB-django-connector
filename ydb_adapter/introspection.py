from typing import List, Dict, Any
from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo, FieldInfo
from django.db import models


class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = {
        'UInt8': models.SmallIntegerField,
        'UInt16': models.IntegerField,
        'UInt32': models.BigIntegerField,
        'UInt64': models.BigIntegerField,
        'Int8': models.SmallIntegerField,
        'Int16': models.SmallIntegerField,
        'Int32': models.IntegerField,
        'Int64': models.BigIntegerField,
        'Float32': models.FloatField,
        'Float64': models.FloatField,
        'String': models.CharField,
        'FixedString': models.CharField,
        'DateTime': models.DateTimeField,
        'Date': models.DateField,
        'Decimal': models.DecimalField,
        'UUID': models.UUIDField,
        'Boolean': models.BooleanField,
        'JSON': models.JSONField
    }

    def get_table_list(self, cursor) -> List[TableInfo]:
        """
        Returns a list of table names in the current database.
        """
        cursor.execute("SHOW TABLES")
        return [TableInfo(row[0], 't') for row in cursor.fetchall()]

    def get_table_description(self, cursor, table_name: str) -> List[FieldInfo]:
        """
        Returns a description of the table, with the DB-API cursor.description interface.
        """
        cursor.execute(f"DESCRIBE {table_name}")
        return [FieldInfo(
            name=row[0],
            type_code=row[1],
            display_size=row[2],
            internal_size=row[3],
            precision=row[4],
            scale=row[5],
            null_ok=row[6],
            default=row[7]
        ) for row in cursor.fetchall()]  # Need to find out if YDB supports collation?

    def get_relations(self, cursor, table_name: str) -> Dict[str, Any]:
        """
        Returns a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        # TODO: Logic to find relations goes here
        return {}

    def get_indexes(self, cursor, table_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Returns a dictionary of field names mapped to their indexes information.
        """
        cursor.execute(f"SHOW INDEX FROM {table_name}")
        indexes = {}
        for row in cursor.fetchall():
            indexes[row['Column_name']] = {
                'primary_key': row['Key_name'] == 'PRIMARY',
                'unique': not row['Non_unique']
            }
        return indexes
