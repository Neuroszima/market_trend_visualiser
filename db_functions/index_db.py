from ast import literal_eval
import psycopg2
from db_helpers import connection_dict, db_string_converter


# create queries
# insert queries
query_insert_index_data = """
"""


def insert_index_historical_data(historical_data: list[dict], index_name: str):
    with psycopg2.connect(**connection_dict) as conn:
        # conn: connection.connection
        # cur: cursor.cursor
        cur = conn.cursor()
        for rownum, candle in enumerate(historical_data):
            query_dict = {
                "index_name": index_name,
                "id": rownum,

            }
            cur.execute(query_insert_index_data.format(**query_dict))
            conn.commit()
        cur.close()

