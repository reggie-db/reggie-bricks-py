import json

from dbx_tools import warehouses

if __name__ == "__main__":
    resp = warehouses.execute_statement(
        statement="SELECT current_user(), current_date();"
    )
    print(json.dumps(resp.as_dict(), indent=2))
