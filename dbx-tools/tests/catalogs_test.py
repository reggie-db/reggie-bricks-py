import pytest

from dbx_tools.catalogs import CatalogSchema, CatalogSchemaTable


def test_catalog_schema_of_from_string():
    schema = CatalogSchema.of("main.analytics")
    assert schema.catalog == "main"
    assert schema.schema == "analytics"


def test_catalog_schema_of_from_list_of_strings():
    schema = CatalogSchema.of(["main", "analytics"])
    assert str(schema) == "main.analytics"


def test_catalog_schema_of_from_list_of_dotted_strings():
    schema = CatalogSchema.of(["main.analytics"])
    assert str(schema) == "main.analytics"


def test_catalog_schema_table_of_from_string():
    table = CatalogSchemaTable.of("main.analytics.orders")
    assert str(table) == "main.analytics.orders"


def test_catalog_schema_table_of_from_list_of_strings():
    table = CatalogSchemaTable.of(["main", "analytics", "orders"])
    assert table.catalog == "main"
    assert table.schema == "analytics"
    assert table.table == "orders"


def test_catalog_schema_table_of_from_list_of_dotted_strings():
    table = CatalogSchemaTable.of(["main.analytics", "orders"])
    assert str(table) == "main.analytics.orders"


def test_catalog_schema_table_of_from_list_of_lists_of_dotted_strings():
    table = CatalogSchemaTable.of(["main.analytics", ["orders"], []])
    assert str(table) == "main.analytics.orders"


def test_catalog_schema_table_of_rejects_missing_table():
    with pytest.raises(ValueError):
        CatalogSchemaTable.of("main.analytics")


def test_catalog_schema_of_rejects_too_many_segments():
    with pytest.raises(ValueError):
        CatalogSchema.of("main.analytics.orders.extra")


def test_catalog_schema_table_of_rejects_too_many_segments():
    with pytest.raises(ValueError):
        CatalogSchemaTable.of("main.analytics.orders.extra")


def test_catalog_schema_of_rejects_list_with_too_many_segments():
    with pytest.raises(ValueError):
        CatalogSchema.of(["main.analytics", "orders.extra"])


def test_catalog_schema_table_of_rejects_empty_segment():
    with pytest.raises(ValueError):
        CatalogSchemaTable.of("main..orders")
