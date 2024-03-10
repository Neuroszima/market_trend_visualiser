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

# fetch queries
# there is a view that we made previously to provide "explainable" data, not just numbers
_query_fetch_markets = "SELECT * FROM public.\"markets_explained\" m {optional_filter};"
_query_fetch_timezones = "SELECT * FROM public.\"timezones\" t {optional_filter};"
_query_fetch_countries = "SELECT * FROM public.\"countries\" c {optional_filter};"
_query_fetch_plans = "SELECT * FROM public.\"plans\" p {optional_filter};"


def insert_countries_(countries: set[str]):
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
        for index, plan in enumerate(sorted(plans)):
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
        for index, market in enumerate(sorted(markets, key=lambda x: x['name'])):
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


def fetch_markets_(
        name_like: str | None = None, market_identification_code_like: str | None = None,
        timezone_name_like: str | None = None, plan_name_like: str | None = None,
        country_name_like: str | None = None):
    """
    Obtain a list of markets that resides in database (using a prepared view)

    Additional options allow for similarities to be searched for, for example market name (example 'NASDAQ')
    and country of origin (country is "full" -> not "US" but "United States").

    Timezone info usually has a city name attached to it.

    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    (example: name_like='NAS%')

    Defaults to yielding entire stored information at once, from a view that explains data
    (using view -> meaning it substitutes raw relation ID's for meaningful human-readable content)
    """
    filter_map = {
        "m.name LIKE '{}'": name_like,
        "m.mic_code LIKE '{}'": market_identification_code_like,
        "m.timezone_name LIKE '{}'": timezone_name_like,
        "m.access_plan LIKE '{}'": plan_name_like,
        "m.country_name LIKE '{}'": country_name_like,
    }
    if any([value == '' for _, value in filter_map.items()]):
        raise ValueError('empty values passed as "" are not valid for the query')
    used_filters = [
        key.format(value) for key, value in filter_map.items() if value is not None
    ]
    if len(used_filters) >= 2:
        optional_filters = "WHERE " + "AND ".join(used_filters)
    elif len(used_filters) == 1:
        optional_filters = "WHERE " + used_filters[0]
    else:
        optional_filters = ""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_markets.format(
            optional_filter=optional_filters
        ))
        m = cur.fetchall()
    return m


def fetch_timezones_(name_like: str | None = None):
    """
    Obtain a list of timezones that resides in database

    Additional options allow for similarities to be searched for namely "name" param
    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    """
    if name_like == "":
        raise ValueError('empty values passed as "" are not valid for the query')
    optional_filter = f"WHERE t.name LIKE '{name_like}'" if name_like else ""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_timezones.format(
            optional_filter=optional_filter,
        ))
        m = cur.fetchall()
    return m


def fetch_countries_(name_like: str | None = None):
    """
    Obtain a list of countries that resides in database

    Additional options allow for similarities to be searched for namely "name" param
    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    """
    if name_like == "":
        raise ValueError('empty values passed as "" are not valid for the query')
    optional_filter = f"WHERE c.name LIKE '{name_like}'" if name_like else ""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_countries.format(
            optional_filter=optional_filter,
        ))
        m = cur.fetchall()
    return m


def fetch_plans_(plan_name_like: str | None = None):
    """
    Obtain a list of countries that resides in database

    Additional options allow for similarities to be searched for namely "name" param
    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    """
    if plan_name_like == "":
        raise ValueError('empty values passed as "" are not valid for the query')
    optional_filter = f"WHERE p.plan LIKE '{plan_name_like}'" if plan_name_like else ""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_plans.format(
            optional_filter=optional_filter,
        ))
        m = cur.fetchall()
    return m
