from ast import literal_eval
from pprint import pprint

import psycopg2
from db_functions.db_helpers import _connection_dict, db_string_converter_


# insert queries
_query_insert_country = "INSERT INTO countries (\"ID\", name) VALUES ({index}, {country_name});"
_query_insert_timezone = "INSERT INTO timezones (\"ID\", name) VALUES ({index}, {timezone_name});"
_query_insert_plans = "INSERT INTO plans (\"ID\", global, plan) VALUES ({index}, {access_global}, {access_plan});"
_query_insert_markets = '''
INSERT INTO public."markets" ("ID", name, access, timezone, country, code)
VALUES (
    {index},
    {market_name},
    (SELECT \"ID\" FROM public.plans p WHERE p.plan = {market_plan}),
    (SELECT \"ID\" FROM public.timezones t WHERE t.name = {market_timezone}),
    (SELECT \"ID\" FROM "public".countries c WHERE c.name = {market_country}),
    {market_code}
);
'''


def insert_countries_(countries: set):
    """
    insert information about countries, that is obtained from TwelveData API provider.

    Usually this information will come in from other sources (like the list of available markets with their
    respective countries), and will be scrapped that way. However, in any unfortunate case of something not having a
    designated country, additional "Unknown" is added to not disrupt other functions. Such case might happen, when
    "null" or "" value is fed from API that could disrupt DB constraints.
    """
    with psycopg2.connect(**_connection_dict) as conn:
        # conn: connection.connection
        # cur: cursor.cursor
        cur = conn.cursor()
        countries.update(('Unknown',))
        for index, c in enumerate(sorted(countries)):
            country = db_string_converter_(c)
            cur.execute(_query_insert_country.format(index=index, country_name=country))
            conn.commit()
        cur.close()


def insert_timezones_(timezones: set[str]):
    """insert into table a unique set of available timezones covered by API"""
    with psycopg2.connect(**_connection_dict) as conn:
        # conn: connection.connection
        # cur: cursor.cursor
        cur = conn.cursor()
        for index, t in enumerate(sorted(timezones)):
            timezone = db_string_converter_(t)
            cur.execute(_query_insert_timezone.format(index=index, timezone_name=timezone))
            conn.commit()
        cur.close()


def insert_plans_(plans: set[str]):
    """
    insert available paid/free subscription plans
    the input set is actually set of str representations of dicts
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, plan in enumerate(plans):
            query_start = literal_eval(plan)
            query_dict = dict()
            for key in query_start:
                query_dict["access_"+key] = db_string_converter_(query_start[key])
            query_dict['index'] = index
            cur.execute(_query_insert_plans.format(**query_dict))
            conn.commit()
        cur.close()


def insert_markets_(markets: list[dict]):
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, market in enumerate(markets):
            country = market['country']
            if country == "":
                country = "Unknown"
            query_dict = {
                "index": index,
                "market_name": db_string_converter_(market['name']),
                "market_code": db_string_converter_(market['code']),
                "market_plan": db_string_converter_(market['access']['plan']),
                "market_timezone": db_string_converter_(market['timezone']),
                "market_country": db_string_converter_(country),
            }
            # print(_query_insert_markets.format(**query_dict))
            cur.execute(_query_insert_markets.format(**query_dict))
            conn.commit()
        cur.close()

