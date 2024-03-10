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

# fetch queries
_query_fetch_investment_types = "SELECT * FROM \"public\".investment_types i_ts {optional_filters};"
_query_fetch_stocks = "SELECT * FROM \"public\".stocks_explained s {optional_filters};"


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
            if stock["country"] == "":
                stock["country"] = "Unknown"
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


def fetch_investment_types_(name_like: str | None = None):
    """
    Obtain a list of investment/equity types that resides in database

    Additional options allow for similarities to be searched for "equity type name" (example being 'Common Stock')
    Add '%' in front or in the end of parameter to increase the likelihood of finding results
    """
    optional_filter = f"WHERE i_ts.name LIKE '{name_like}'" if name_like else ""
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_query_fetch_investment_types.format(
            optional_filters=optional_filter,
        ))
        f_gs = cur.fetchall()
    return f_gs


def fetch_stocks_(
        symbol_like: str | None = None, name_like: str | None = None,
        currency_symbol_like: str | None = None, market_identification_code_like: str | None = None,
        country_name_like: str | None = None, investment_type_name_like: str | None = None,
        access_plan_name_like: str | None = None):
    """
    Obtain a list of currencies that resides in database (using prepared view)
    Add '%' in front or in the end of parameter to increase the likelihood of finding results

    Additional options allow for similarities to be searched for, for example investment type name
    and "mic" symbol, as the exchange at which instrument is traded.
    Defaults to yielding entire stored information at once.
    (using view -> it substitutes raw relation ID's for human-readable content)
    """
    filter_map = {
        "s.symbol LIKE '{}'": symbol_like,
        "s.name LIKE '{}'": name_like,
        "s.currency_symbol LIKE '{}'": currency_symbol_like,
        "s.exchange_mic_code LIKE '{}'": market_identification_code_like,
        "s.country_name LIKE '{}'": country_name_like,
        "s.investment_type LIKE '{}'": investment_type_name_like,
        "s.access_plan LIKE '{}'": access_plan_name_like,
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
        cur.execute(_query_fetch_stocks.format(
            optional_filters=optional_filters,
        ))
        f_ps = cur.fetchall()
    return f_ps

