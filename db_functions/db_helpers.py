import psycopg2

import settings


# helper queries
information_schema = "select * from information_schema.\"tables\" where table_schema LIKE 'public';"
information_schema2 = "select * from information_schema.\"columns\" where table_schema LIKE 'public';"

# select table_name from information_schema.\"tables\" where table_name = {table_name}
information_schema_time_series_check = """
SELECT table_name, table_schema from information_schema.\"tables\" 
where table_name like {table_name} and table_schema = {time_series_schema};
"""

# connection dict
connection_dict = {
    "database": settings.DB_NAME,
    "password": settings.DB_PASSWORD,
    "user": settings.DB_USER
}


# helper errors
class TimeSeriesNotFoundError(Exception):
    pass


# functions
def db_string_converter(string: str):
    if "'" in string:
        to_send = " || chr(39) || ".join(["'" + s.encode("UTF-8").decode() + "'" for s in string.split("'")])
    else:
        to_send = "'" + string + "'"
    return to_send

