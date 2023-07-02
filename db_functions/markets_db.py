from ast import literal_eval
import psycopg2
from db_helpers import connection_dict, db_string_converter


# insert queries
query_insert_country = "INSERT INTO countries (\"ID\", name) VALUES ({index}, {country_name});"
query_insert_timezone = "INSERT INTO timezones (\"ID\", name) VALUES ({index}, {timezone_name});"
query_insert_plans = "INSERT INTO plans (\"ID\", global, plan) VALUES ({index}, {access_global}, {access_plan});"
query_insert_markets = '''
INSERT INTO public."markets" ("ID", name, access, timezone, country, code)
VALUES (
    {index},
    {market_name},
    (SELECT "ID" FROM public.plans p WHERE p.plan = {market_plan}),
    (SELECT "ID" FROM public.timezones t WHERE t.name = {market_timezone}),
    (SELECT "ID" FROM "public".countries c WHERE c.name = {market_country}),
    {market_code}
);
'''


def insert_countries(countries: set):
    with psycopg2.connect(**connection_dict) as conn:
        # conn: connection.connection
        # cur: cursor.cursor
        cur = conn.cursor()
        for index, c in enumerate(sorted(countries)):
            country = db_string_converter(c)
            cur.execute(query_insert_country.format(index=index, country_name=country))
            conn.commit()
        cur.close()


def insert_timezones(timezones: set[str]):
    """insert into table a unique set of available timezones covered by API"""
    with psycopg2.connect(**connection_dict) as conn:
        # conn: connection.connection
        # cur: cursor.cursor
        cur = conn.cursor()
        for index, t in enumerate(sorted(timezones)):
            timezone = db_string_converter(t)
            cur.execute(query_insert_timezone.format(index=index, timezone_name=timezone))
            conn.commit()
        cur.close()


def insert_plans(plans: set[str]):
    """
    insert available paid/free subscription plans
    the input set is actually set of str representations of dicts
    """
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, plan in enumerate(plans):
            query_start = literal_eval(plan)
            query_dict = dict()
            for key in query_start:
                query_dict["access_"+key] = db_string_converter(query_start[key])
            query_dict['index'] = index
            cur.execute(query_insert_plans.format(**query_dict))
            conn.commit()
        cur.close()


def insert_markets(markets: list[dict]):
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, market in enumerate(markets):
            query_dict = {
                "index": index,
                "market_name": db_string_converter(market['name']),
                "market_code": db_string_converter(market['code']),
                "market_plan": db_string_converter(market['access']['plan']),
                "market_timezone": db_string_converter(market['timezone']),
                "market_country": db_string_converter(market['country'])
            }
            print(query_insert_markets.format(**query_dict))
            cur.execute(query_insert_markets.format(**query_dict))
            conn.commit()
        cur.close()


if __name__ == '__main__':
    with open("markets_plans.txt", 'r', newline='\n', encoding='UTF-8') as markets_text:
        data = literal_eval(markets_text.read())['data']

    plans = set()
    countries = set()
    timezones = set()

    for e in data:
        access_obj = {
            'plan': e["access"]['plan'],
            "global": e["access"]['global'],
        }
        plans.update((str(access_obj),))
        countries.update((str(e['country']),))
        timezones.update((str(e['timezone']),))

    insert_timezones(timezones)
    insert_countries(countries)
    insert_plans(plans)
    insert_markets(data)
