from ast import literal_eval
import psycopg2
from db_helpers import connection_dict, db_string_converter


# insert queries
query_insert_currency = """
INSERT INTO currencies (\"ID\", name, symbol) 
VALUES ({index}, {currency_name}, {currency_symbol});"""
query_insert_forex_currency_group = """
INSERT INTO forex_currency_groups (\"ID\", name)
VALUES ({index}, {currency_group})"""
query_insert_forex_pair = """
insert into "public".forex_pairs (\"ID\", currency_group, symbol, currency_base, currency_quote)
values (
    {index},
    (select grps.\"ID\" from "public".forex_currency_groups grps where grps."name" = {currency_group}),
    {symbol},
    (select cur.\"ID\" from "public".currencies cur where cur."name" = {currency_base_name}),
    (select cur2.\"ID\" from "public".currencies cur2 where cur2."name" = {currency_quote_name})
);
"""


def insert_currencies(currencies: set[str]):
    """fill currencies table with all the available currencies from TwelveData API"""
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, c in enumerate(sorted(currencies)):
            currency: dict = literal_eval(c)
            currency_name = db_string_converter(currency['name'])
            currency_symbol = db_string_converter(currency['symbol'])
            cur.execute(query_insert_currency.format(
                index=index, currency_name=currency_name, currency_symbol=currency_symbol))
            conn.commit()
        cur.close()


def insert_forex_currency_groups(forex_currency_groups: set[str]):
    """fill currencies table with all the available currency groups from TwelveData API"""
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, g in enumerate(sorted(forex_currency_groups)):
            currency_group_s = db_string_converter(g)
            cur.execute(query_insert_forex_currency_group.format(
                index=index, currency_group=currency_group_s))
            conn.commit()
        cur.close()


def insert_forex_pairs_available(pairs: list[dict]):
    """fill currencies table with all the tradeable currency pairs covered by TwelveData API"""
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, pair_dict in enumerate(pairs):
            query_dict = {
                "index": index,
                "currency_group": db_string_converter(pair_dict['currency_group']),
                "symbol": db_string_converter(pair_dict['symbol']),
                "currency_base_name": db_string_converter(pair_dict['currency_base']),
                "currency_quote_name": db_string_converter(pair_dict['currency_quote']),
            }
            cur.execute(query_insert_forex_pair.format(**query_dict))
            conn.commit()
        cur.close()


if __name__ == '__main__':
    sample_data = [
        {'symbol': 'AED/BRL', 'currency_group': 'Exotic-Cross', 'currency_base': 'UAE Dirham',
         'currency_quote': 'Brazil Real'},
        {'symbol': 'JPY/XOF', 'currency_group': 'Exotic-Cross', 'currency_base': 'Japanese Yen',
         'currency_quote': 'West African CFA franc'},
        {'symbol': 'JPY/ZAR', 'currency_group': 'Exotic-Cross', 'currency_base': 'Japanese Yen',
         'currency_quote': 'South African Rand'},
        {'symbol': 'KGS/RUB', 'currency_group': 'Minor', 'currency_base': 'Kyrgyzstan som',
         'currency_quote': 'Russian Ruble'},
    ]

    currencies = set()
    currency_groups = set()

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
        currencies.update((str(currency_base_entry), ))
        currencies.update((str(currency_quote_entry), ))

        # groups
        currency_groups.update((str(forex_pair_data['currency_group']), ))

    insert_forex_currency_groups(currency_groups)
    insert_currencies(currencies)
    insert_forex_pairs_available(sample_data)
