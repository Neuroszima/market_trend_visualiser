from db_functions.db_helpers import _connection_dict

import psycopg2


drop_currencies = "DROP TABLE IF EXISTS \"public\".currencies CASCADE;"
drop_markets = "DROP TABLE IF EXISTS \"public\".markets CASCADE;"
drop_investment_types = "DROP TABLE IF EXISTS \"public\".investment_types CASCADE;"
drop_forex_currency_groups = "DROP TABLE IF EXISTS \"public\".forex_currency_groups CASCADE;"
drop_timezones = "DROP TABLE IF EXISTS \"public\".timezones CASCADE;"
drop_stocks = "DROP TABLE IF EXISTS \"public\".stocks CASCADE;"
drop_time_tracking_info = "DROP TABLE IF EXISTS \"public\".time_tracking_info CASCADE;"
drop_forex_pairs = "DROP TABLE IF EXISTS \"public\".forex_pairs CASCADE;"
drop_plans = "DROP TABLE IF EXISTS \"public\".plans CASCADE;"
drop_countries = "DROP TABLE IF EXISTS \"public\".countries CASCADE;"

drop_schema_1day = "DROP SCHEMA IF EXISTS \"1day_time_series\" CASCADE;"
drop_schema_1min = "DROP SCHEMA IF EXISTS \"1min_time_series\" CASCADE;"

drop_tracking_view = "DROP VIEW IF EXISTS \"public\".tracked_indexes;"

delete_function_1day_view = "DROP FUNCTION IF EXISTS \"public\".generate_financial_view_1day;"
delete_function_1min_view = "DROP FUNCTION IF EXISTS \"public\".generate_financial_view_1min;"

delete_db_user = "DROP USER IF EXISTS db_user;"


def purge_db_structure_():
    """remove every element from the database, for testing purposes"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur: psycopg2._psycopg.cursor = conn.cursor()
        cur.execute(drop_tracking_view)
        cur.execute(drop_time_tracking_info)
        cur.execute(drop_stocks)
        cur.execute(drop_investment_types)
        cur.execute(drop_markets)
        cur.execute(drop_plans)
        cur.execute(drop_countries)
        cur.execute(drop_timezones)
        cur.execute(drop_forex_pairs)
        cur.execute(drop_currencies)
        cur.execute(drop_forex_currency_groups)

        cur.execute(drop_schema_1day)
        cur.execute(drop_schema_1min)

        cur.execute(delete_function_1day_view)
        cur.execute(delete_function_1min_view)

        cur.execute(delete_db_user)


if __name__ == '__main__':
    purge_db_structure_()