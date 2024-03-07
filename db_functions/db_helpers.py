import psycopg2
import settings


# helper queries
_information_schema = "select * from information_schema.\"tables\" where table_schema LIKE 'public';"
_information_schema2 = "select * from information_schema.\"columns\" where table_schema LIKE 'public';"
_table_rows_quantity = "select count(*) from \"{schema}\".\"{table_name}\";"
_last_row_id = "select \"ID\" from \"{schema}\".\"{table_name}\" tab order by tab.\"ID\" DESC LIMIT 1;"
_exist_in_stocks = "select public.check_is_stock('{symbol}')"
_delete_single_based_on_ID = "DELETE FROM \"{schema_name}\".\"{table_name}\" tab WHERE tab.\"ID\" = {index};"

# select table_name from information_schema.\"tables\" where table_name = {table_name}
_information_schema_table_check = """
SELECT table_name, table_schema from information_schema.\"tables\" 
where table_name like {table_name} and table_schema = {schema};
"""
_information_schema_check = "select schema_name from \"information_schema\".schemata;"
_information_schema_function_check = "select * from \"public\".non_standard_functions;"

# connection dict
_connection_dict = {
    "database": settings.DB_NAME,
    "password": settings.DB_PASSWORD,
    "user": settings.DB_USER
}


# helper errors
class TimeSeriesNotFoundError_(Exception):
    pass


class TimeSeriesExistsError_(Exception):
    pass


class DataNotPresentError_(Exception):
    pass


# functions
def db_string_converter_(string: str):
    """format query parameter that needs to have a special characters in it"""
    if "'" in string:
        to_send = " || chr(39) || ".join(["'" + s.encode("UTF-8").decode() + "'" for s in string.split("'")])
    else:
        to_send = "'" + string + "'"
    return to_send


def last_row_ID_(schema_name: str, table_name: str):
    """obtain the last rows ID form a specified table"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_last_row_id.format(schema=schema_name, table_name=table_name))
        res = cur.fetchall()
    return res[0][0]


def is_stock_(symbol: str) -> bool:
    """search if the symbol already exist in stocks table. If not, assume forex pair"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_exist_in_stocks.format(symbol=symbol))
        res = cur.fetchall()
    return res[0][0]


def list_nonstandard_functions_():
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_information_schema_function_check)
        res = cur.fetchall()
    return tuple(r for r in res)


if __name__ == '__main__':
    print(list_nonstandard_functions_())
