import psycopg2
from psycopg2._psycopg import Error

from db_functions.db_helpers import db_string_converter_, _connection_dict


# insert queries
query_insert_equity_type = "INSERT INTO \"public\".investment_types (\"ID\", name) VALUES ({index}, {equity_name});"
query_insert_stocks = """
INSERT INTO \"public\".stocks (\"ID\", symbol, name, currency, exchange, country, type, plan)
VALUES (
    {index},
    {stock_symbol},
    {stock_name},
    (SELECT \"ID\" FROM public."currencies" curr WHERE curr."symbol" = {currency_symbol}),
    (SELECT \"ID\" FROM public."markets" m WHERE m."code" = {exchange_code}),
    (SELECT \"ID\" FROM public."countries" cntrs WHERE cntrs."name" = {stock_country}),
    (SELECT \"ID\" FROM public."investment_types" inv_ts WHERE inv_ts."name" = {equity_name}),
    (SELECT \"ID\" FROM public."plans" p WHERE p."plan" = {access_plan})
);
"""


def insert_investment_types_(equity_types):
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, t in enumerate(sorted(equity_types)):
            equity_name = db_string_converter_(t)
            cur.execute(query_insert_equity_type.format(index=index, equity_name=equity_name))
            conn.commit()
        cur.close()


def insert_stocks_(stocks: list[dict]):
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        for index, stock in enumerate(stocks):
            if index % 1000 == 0:
                print(index)
            query_dict = {
                "index": index,
                "stock_symbol": db_string_converter_(stock["symbol"]),
                "stock_name": db_string_converter_(stock["name"]),
                # upper prevents abominations like "GBp"
                "currency_symbol": db_string_converter_(stock["currency"].upper()),
                "exchange_code": db_string_converter_(stock["mic_code"]),
                "stock_country": db_string_converter_(stock["country"]),
                "equity_name": db_string_converter_(stock["type"]),
                "access_plan": db_string_converter_(stock["access"]["plan"]),
            }
            try:
                cur.execute(query_insert_stocks.format(**query_dict))
            except Error as e:
                print(e)
                print(query_insert_stocks.format(**query_dict))
                raise e
            conn.commit()
        cur.close()
    

if __name__ == '__main__':
    sample_data = [
        {'symbol': 'AADV', 'name': 'Albion Development VCT PLC', 'currency': 'GBp', 'exchange': 'LSE',
         'mic_code': 'XLON', 'country': 'United Kingdom', 'type': 'Common Stock',
         'access': {'global': 'Level A', 'plan': 'Grow'}},
        {'symbol': 'AAPL', 'name': 'Apple Inc', 'currency': 'USD', 'exchange': 'NASDAQ',
         'mic_code': 'XNGS', 'country': 'United States', 'type': 'Common Stock',
         'access': {'global': 'Basic', 'plan': 'Basic'}},
        {'symbol': 'NVDA', 'name': 'NVIDIA Corp', 'currency': 'USD', 'exchange': 'NASDAQ',
         'mic_code': 'XNGS', 'country': 'United States', 'type': 'Common Stock',
         'access': {'global': 'Basic', 'plan': 'Basic'}},
        {'symbol': 'OTEX', 'name': 'Open Text Corp', 'currency': 'USD', 'exchange': 'NASDAQ',
         'mic_code': 'XNGS', 'country': 'United States', 'type': 'Common Stock',
         'access': {'global': 'Basic', 'plan': 'Basic'}}
    ]

    equity_types = set()

    for e in sample_data:
        equity_types.update((str(e['type']),))

    insert_investment_types_(equity_types)
    insert_stocks_(sample_data)

