import psycopg2

from db_functions import db_helpers


# queries
create_time_table = """
create table {time_interval}_time_series."{symbol}_{market_identification_code}" (
    "ID" integer not null,
    datetime timestamp without time zone,
    open numeric(10,5),
    close numeric(10,5),
    high numeric(10,5),
    low numeric(10,5),
    volume integer,
    constraint {lower_symbol}_time_series_pkey primary key("ID")
);
"""


def time_series_table_exists(symbol: str, market_identification_code: str, time_interval: str):
    with psycopg2.connect(**db_helpers.connection_dict) as conn:
        cur = conn.cursor()
        table_name = db_helpers.db_string_converter(f"{symbol}_{market_identification_code}")
        time_series_schema = f'{time_interval}_time_series'
        cur.execute(db_helpers.information_schema_time_series_check.format(
            table_name=table_name,
            time_series_schema=db_helpers.db_string_converter(time_series_schema),
        ))
        result = cur.fetchall()
        if result:
            return True
        return False


def create_time_series(symbol: str, market_identification_code: str, time_interval: str):
    if time_interval not in ["1min", "1day"]:
        raise ValueError("Improper argument for this query. Possible intervals: ('1min', '1day')")
    with psycopg2.connect(**db_helpers.connection_dict) as conn:
        cur = conn.cursor()
        q_dict = {
            "market_identification_code": market_identification_code,
            "symbol": symbol,
            "lower_symbol": symbol.lower(),
            "time_interval": time_interval,
        }
        cur.execute(create_time_table.format(**q_dict))
    assert time_series_table_exists(symbol, market_identification_code, time_interval)


if __name__ == '__main__':
    assert time_series_table_exists("OTEX", "XNGS", "1min")
    params = {
        "symbol": "NVDA",
        "market_identification_code": "XNGS",
        "time_interval": "1min",
    }
    create_time_series(**params)
