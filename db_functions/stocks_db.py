from ast import literal_eval

import psycopg2
from psycopg2._psycopg import Error

from db_helpers import db_string_converter, connection_dict


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


def insert_investment_types(equity_types):
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, t in enumerate(sorted(equity_types)):
            equity_name = db_string_converter(t)
            cur.execute(query_insert_equity_type.format(index=index, equity_name=equity_name))
            conn.commit()
        cur.close()


def insert_stocks(stocks: list[dict]):
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        for index, stock in enumerate(stocks):
            if index % 1000 == 0:
                print(index)
            if any([

            ]): continue
            query_dict = {
                "index": index,
                "stock_symbol": db_string_converter(stock["symbol"]),
                "stock_name": db_string_converter(stock["name"]),
                "currency_symbol": db_string_converter(stock["currency"].upper()),
                "exchange_code": db_string_converter(stock["mic_code"]),
                "stock_country": db_string_converter(stock["country"]),
                "equity_name": db_string_converter(stock["type"]),
                "access_plan": db_string_converter(stock["access"]["plan"]),
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
        {'symbol': '06MA', 'name': 'Materialise NV', 'currency': 'EUR', 'exchange': 'FSX',
         'mic_code': 'XFRA', 'country': 'Germany', 'type': 'American Depositary Receipt',
         'access': {'global': 'Level A', 'plan': 'Grow'}},
        {'symbol': '000001', 'name': 'Ping An Bank Co., Ltd.', 'currency': 'CNY', 'exchange': 'SZSE',
         'mic_code': 'XSHE', 'country': 'China', 'type': 'Common Stock',
         'access': {'global': 'Level B', 'plan': 'Pro'}},
        {'symbol': '0R2C', 'name': 'Endeavour Silver Corp.', 'currency': 'CAD', 'exchange': 'LSE',
         'mic_code': 'XLON', 'country': 'United Kingdom', 'type': 'Common Stock',
         'access': {'global': 'Level A', 'plan': 'Grow'}},
        {'symbol': '0R2D', 'name': 'Kinross Gold Corporation', 'currency': 'CAD', 'exchange': 'LSE',
         'mic_code': 'XLON', 'country': 'United Kingdom', 'type': 'Common Stock',
         'access': {'global': 'Level A', 'plan': 'Grow'}},
        {'symbol': '0R2E', 'name': 'Union Pacific Corporation', 'currency': 'USD', 'exchange': 'LSE',
         'mic_code': 'XLON', 'country': 'United Kingdom', 'type': 'Common Stock',
         'access': {'global': 'Level A', 'plan': 'Grow'}},
    ]

    equity_types = set()

    for e in sample_data:
        equity_types.update((str(e['type']),))

    insert_investment_types(equity_types)
    # Freetrailer Group AS - do not have country
    # Whoosh Holding PAO - do not have country
    insert_stocks(sample_data)

