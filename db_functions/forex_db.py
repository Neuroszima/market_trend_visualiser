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
    (select cur.\"ID\" from "public".currencies cur where cur."name" = {currency_base_name}),
    (select cur2.\"ID\" from "public".currencies cur2 where cur2."name" = {currency_quote_name})
);
"""


def insert_currencies_(currencies: set[str]):
    """fill currencies table with all the available currencies from TwelveData API"""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, c in enumerate(sorted(currencies)):
            currency: dict = literal_eval(c)
            currency_name = db_string_converter_(currency['name'])
            currency_symbol = db_string_converter_(currency['symbol'])
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
            query_dict = {
                "index": index,
                "currency_group": db_string_converter_(pair_dict['currency_group']),
                "symbol": db_string_converter_(pair_dict['symbol']),
                "currency_base_name": db_string_converter_(pair_dict['currency_base']),
                "currency_quote_name": db_string_converter_(pair_dict['currency_quote']),
            }
            cur.execute(_query_insert_forex_pair.format(**query_dict))
            conn.commit()
        cur.close()


if __name__ == '__main__':
    sample_data = [
        {'symbol': 'AED/BRL', 'currency_group': 'Exotic-Cross', 'currency_base': 'UAE Dirham',
         'currency_quote': 'Brazil Real'},
        {'symbol': 'JPY/ZAR', 'currency_group': 'Exotic-Cross', 'currency_base': 'Japanese Yen',
         'currency_quote': 'South African Rand'},
        {'symbol': 'ARS/USD', 'currency_group': 'Exotic', 'currency_base': 'Argentinian Peso',
         'currency_quote': 'US Dollar'},
        {'symbol': 'KGS/RUB', 'currency_group': 'Minor', 'currency_base': 'Kyrgyzstan som',
         'currency_quote': 'Russian Ruble'},
        {'symbol': 'USD/EUR', 'currency_group': 'Major', 'currency_base': 'US Dollar',
         'currency_quote': 'Euro'},
        {'symbol': 'USD/GBP', 'currency_group': 'Major', 'currency_base': 'US Dollar',
         'currency_quote': 'British Pound'},
        {'symbol': 'USD/JPY', 'currency_group': 'Major', 'currency_base': 'US Dollar',
         'currency_quote': 'Japanese Yen'}
    ]

    currencies__ = set()
    currency_groups__ = set()

    # print(data[0])
    for forex_pair_data in sample_data:

        # currencies
        currency_symbols: list = forex_pair_data['symbol'].split('/')
        currency_base_symbol = currency_symbols[0]
        currency_quote_symbol = currency_symbols[1]
        currency_base_entry = {
            'name': forex_pair_data['currency_base'],
            'symbol': currency_base_symbol
        }
        currency_quote_entry = {
            'name': forex_pair_data['currency_quote'],
            "symbol": currency_quote_symbol,
        }
        currencies__.update((str(currency_base_entry),))
        currencies__.update((str(currency_quote_entry),))

        # groups
        currency_groups__.update((str(forex_pair_data['currency_group']),))

    insert_forex_currency_groups_(currency_groups__)
    insert_currencies_(currencies__)
    insert_forex_pairs_available_(sample_data)
