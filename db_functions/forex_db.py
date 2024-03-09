from ast import literal_eval
import psycopg2
from db_functions.db_helpers import _connection_dict, db_string_converter_


# insert queries
_query_insert_currency = """
INSERT INTO \"public\".currencies (\"ID\", name, symbol) 
VALUES ({index}, {currency_name}, {currency_symbol});"""
_query_insert_forex_currency_group = """
INSERT INTO \"public\".forex_currency_groups (\"ID\", name)
VALUES ({index}, {currency_group})"""
_query_insert_forex_pair = """
insert into "public".forex_pairs (\"ID\", currency_group, symbol, currency_base, currency_quote)
values (
    {index},
    (select grps.\"ID\" from "public".forex_currency_groups grps where grps."name" = {currency_group}),
    {symbol},
    (select cur.\"ID\" from "public".currencies cur where cur."symbol" = {currency_base_symbol}),
    (select cur2.\"ID\" from "public".currencies cur2 where cur2."symbol" = {currency_quote_symbol})
);
"""

# fetch queries
_query_fetch_currencies = "SELECT * FROM \"public\".currencies c {optional_filters};"
_query_fetch_currency_groups = "SELECT * FROM \"public\".forex_currency_groups f_c_g {optional_filters};"
_query_fetch_forex_pairs = "SELECT * FROM \"public\".forex_pairs_explained f_p {optional_filters};"


def insert_currencies_(currencies: set[str]):
    """fill currencies table with all the available currencies from TwelveData API"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, c in enumerate(sorted(currencies)):
            currency: dict = literal_eval(c)
            currency_name = db_string_converter_(currency['name'])
            currency_symbol = db_string_converter_(currency['symbol'].upper())
            cur.execute(_query_insert_currency.format(
                index=index, currency_name=currency_name, currency_symbol=currency_symbol))
            conn.commit()
        cur.close()


def insert_forex_currency_groups_(forex_currency_groups: set[str]):
    """fill currencies table with all the available currency groups from TwelveData API"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, g in enumerate(sorted(forex_currency_groups)):
            currency_group_s = db_string_converter_(g)
            cur.execute(_query_insert_forex_currency_group.format(
                index=index, currency_group=currency_group_s))
            conn.commit()
        cur.close()


def insert_forex_pairs_available_(pairs: list[dict]):
    """fill currencies table with all the tradeable currency pairs covered by TwelveData API"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, pair_dict in enumerate(pairs):
            base_symbol, quote_symbol = pair_dict['symbol'].split("/")
            query_dict = {
                "index": index,
                "currency_group": db_string_converter_(pair_dict['currency_group']),
                "symbol": db_string_converter_(pair_dict['symbol'].upper()),
                "currency_base_symbol": db_string_converter_(base_symbol.upper()),
                "currency_quote_symbol": db_string_converter_(quote_symbol.upper()),
            }
            cur.execute(_query_insert_forex_pair.format(**query_dict))
            conn.commit()
        cur.close()


def fetch_currencies_(symbol_like: str | None = None, name_like: str | None = None):
    """
    Obtain a list of currencies that resides in database

    Additional options allow for similarities to be searched for, for example symbol similarity
    and country of origin. Defaults to yielding entire stored information at once, from a view that explains data
    (meaning it substitutes raw relation ID's for meaningful names)
    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    """
    optional_filter_1 = f"c.symbol LIKE '{symbol_like}'" if symbol_like else ""
    optional_filter_2 = f"c.name LIKE '{name_like}'" if name_like else ""
    if optional_filter_1 and optional_filter_2:
        optional_filters = f"WHERE {optional_filter_1} AND {optional_filter_2}"
    elif optional_filter_1:
        optional_filters = f"WHERE {optional_filter_1}"
    elif optional_filter_2:
        optional_filters = f"WHERE {optional_filter_2}"
    else:
        optional_filters = ""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_currencies.format(
            optional_filters=optional_filters
        ))
        c = cur.fetchall()
    return c


def fetch_forex_currency_groups_(name_like: str | None = None):
    """
    Obtain a list of timezones that resides in database

    Additional options allow for similarities to be searched for "currency group name" (example being 'Exotic')
    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    """
    optional_filter = f"WHERE f_c_g.name LIKE '{name_like}'"
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_currency_groups.format(
            optional_filters=optional_filter,
        ))
        f_gs = cur.fetchall()
    return f_gs


def fetch_forex_pairs_(
        currency_group_name_like: str | None = None, symbol_like: str | None = None,
        currency_base_symbol_like: str | None = None, currency_quote_symbol_like: str | None = None,
        currency_base_name_like: str | None = None, currency_quote_name_like: str | None = None):
    """
    Obtain a list of currencies that resides in database (using prepared view)
    Add '%' in front or in the end of parameter to increase the likelihood of finding results

    Additional options allow for similarities to be searched for, for example currency group name
    and symbol of the currency. Defaults to yielding entire stored information at once.
    (it substitutes raw relation ID's for human-readable content)
    """
    filter_map = {
        "f_p.currency_group_name LIKE '{}'": currency_group_name_like,
        "f_p.symbol LIKE '{}'": symbol_like,
        "f_p.base_currency_symbol LIKE '{}'": currency_base_symbol_like,
        "f_p.quote_currency_symbol LIKE '{}'": currency_quote_symbol_like,
        "f_p.base_currency_name LIKE '{}'": currency_base_name_like,
        "f_p.quote_currency_name LIKE '{}'": currency_quote_name_like,
    }
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
        cur.execute(_query_fetch_forex_pairs.format(
            optional_filters=optional_filters,
        ))
        f_ps = cur.fetchall()
    return f_ps
