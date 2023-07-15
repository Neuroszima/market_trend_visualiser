import psycopg2

import settings


# helper queries
information_schema = "select * from information_schema.\"tables\" where table_schema LIKE 'public';"
information_schema2 = "select * from information_schema.\"columns\" where table_schema LIKE 'public';"
time_series_check = """
select table_name from information_schema.\"tables\" where table_name = '{symbol}_{market_identification_code}'
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


def time_series_table_exists(symbol, market_identification_code):
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        q_dict = {
            "market_identification_code": market_identification_code,
            "symbol": symbol,
        }
        cur.execute(time_series_check.format(**q_dict))
        result = cur.fetchall()
        if result:
            return True
        return False


# functions
def db_string_converter(string: str):
    if "'" in string:
        to_send = " || chr(39) || ".join(["'" + s.encode("UTF-8").decode() + "'" for s in string.split("'")])
    else:
        to_send = "'" + string + "'"
    return to_send


if __name__ == '__main__':
    assert time_series_table_exists("OTEX", "XNGS")
