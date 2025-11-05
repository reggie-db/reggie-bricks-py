"""
Quick tests for `reggie_tools.catalogs` moved out of the source module.
"""

from reggie_tools.catalogs import catalog_schema, catalog_schema_table

if __name__ == "__main__":
    print(catalog_schema_table("reggie_pierce.racetrac.raw"))
    print(catalog_schema())
