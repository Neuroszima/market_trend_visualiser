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
