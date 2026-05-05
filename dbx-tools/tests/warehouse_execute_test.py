import asyncio

from dbx_tools import warehouses


async def main():
    warehouse = warehouses.get()
    print(warehouse)
    resp = await warehouses.execute_statement_async(
        statement="SELECT current_user(), current_date();", warehouse_id=warehouse.id
    )
    print(resp)


if __name__ == "__main__":
    asyncio.run(main())
