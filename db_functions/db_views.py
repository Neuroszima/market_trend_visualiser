import psycopg2

from db_functions.db_helpers import _connection_dict, TimeSeriesNotFoundError_
from db_functions.time_series_db import time_series_table_exists_

# create queries
_query_create_view = "select {db_create_view_function}('{table_of_origin}');"

# select queries
_query_list_views = """
select table_schema, table_name from information_schema.views 
where table_schema not in ('pg_catalog', 'information_schema');
"""
_query_check_view_existance = """select exists (select * from nonstandard_views 
where table_name = '{view_name}' and table_schema = '{schema_name}');"""


def create_financial_view_(symbol: str, time_interval: str, is_equity: bool, mic_code: str | None = None):
    """
    depending on the inputs, create a database view of the given tables with additional 2 dynamic columns :)

    these columns are nothing special but can ease the drawing process of the charts. Also some new columns
    might be added to reference, for example, market hours and thus make more correlations
    """
    # to cover:
    # generate_financial_view_1day | FUNCTION
    # generate_financial_view_1min | FUNCTION
    # generate_forex_view | FUNCTION
    if not time_series_table_exists_(
            symbol=symbol, time_interval=time_interval, is_equity=is_equity, mic_code=mic_code):
        raise TimeSeriesNotFoundError_("can't create a view for a non-existent table")
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        if is_equity:
            create_params = {
                "db_create_view_function": f"generate_financial_view_{time_interval}",
                "table_of_origin": f"{symbol}_{mic_code}"
            }
        else:
            # currency pair always has 2 parts when split with "/"
            create_params = {
                "db_create_view_function": f"generate_forex_view",
                "table_of_origin": f"%s_%s_%s" % (*symbol.split("/"), time_interval)  # noqa
            }
        cur.execute(_query_create_view.format(**create_params))


def list_nonstandard_views_():
    """list all the views that happen to be in the database, that aren't pg-related"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_list_views)
        res = cur.fetchall()
    return tuple(r for r in res)


def view_exists_(view_name: str, schema_name: str) -> bool:
    """check if given view really exist in database"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_check_view_existance.format(view_name=view_name, schema_name=schema_name))
        res = cur.fetchall()
    return res[0][0]  # unpacks PGSQL boolean

