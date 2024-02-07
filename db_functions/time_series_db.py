from datetime import datetime

import psycopg2

from db_functions import db_helpers
from minor_modules.helpers import time_interval_sanitizer


# create queries
create_time_table = """
create table "{time_interval}_time_series"."{symbol}_{market_identification_code}" (
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

# insert queries
query_insert_equity_data = """
INSERT INTO {time_series_schema}.\"{equity_symbol}_{market_identification_code}\" 
(\"ID\", datetime, open, close, high, low, volume) 
VALUES (
    {index},
    {timestamp}:: timestamp without time zone,
    {open_price},
    {close_price},
    {high_price},
    {low_price},
    {volume}
);
"""  # the ":: timestamp.." is comment in PSQL, so it's ok

# select queries
last_timetable_point = """
SELECT series.datetime FROM "{time_interval}_time_series"."{symbol}_{market_identification_code}" series 
ORDER BY series."ID" DESC LIMIT 1
"""


@time_interval_sanitizer()
def insert_equity_historical_data(historical_data: list[dict], equity_symbol: str, mic_code: str, time_interval: str):
    with psycopg2.connect(**db_helpers.connection_dict) as conn:
        cur = conn.cursor()
        #  iterate from oldest to newest - new rows will be appended to the farthest row anyway
        for rownum, candle in enumerate(historical_data[::-1]):
            query_dict = {
                "index": rownum,
                "equity_symbol": equity_symbol,
                "time_series_schema": f'"{time_interval}_time_series"',
                "market_identification_code": mic_code,
                "timestamp": f"'{candle['datetime']}'",
                "open_price": candle['open'],
                "close_price": candle['close'],
                "high_price": candle['high'],
                "low_price": candle['low'],
                "volume": candle['volume'],
            }
            cur.execute(query_insert_equity_data.format(**query_dict))
            conn.commit()
        cur.close()


@time_interval_sanitizer()
def time_series_table_exists(symbol: str, market_identification_code: str, time_interval: str) -> bool:
    """
    check if the given table exists in the time-specific schema
    """
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


@time_interval_sanitizer()
def create_time_series(symbol: str, market_identification_code: str, time_interval: str) -> None:
    """
    creates table inside database schema, that corresponds to time_interval passed into function call

    each time interval has corresponding database schema that saves stock market price history
    for the given symbol/MIC pair
    """
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


@time_interval_sanitizer()
def time_series_latest_timestamp(symbol: str, market_identification_code: str, time_interval: str) -> datetime:
    """
    grab the latest datapoint from a timeseries and extract the data from it in the format ready to be
    consumed by API loader
    """
    with psycopg2.connect(**db_helpers.connection_dict) as conn:
        cur = conn.cursor()
        q_dict = {
            "market_identification_code": market_identification_code,
            "symbol": symbol,
            "time_interval": time_interval,
        }
        cur.execute(
            last_timetable_point.format(**q_dict))
        last_record = cur.fetchall()
        t_: datetime = last_record[0][0]  # this will already be a "datetime.datetime()" python object
        return t_


if __name__ == '__main__':
    stock_, market_identification_code_ = "OTEX", "XNGS"
    assert time_series_table_exists(stock_, market_identification_code_, "1min")
    t = time_series_latest_timestamp(stock_, market_identification_code_, "1min")
    print(t)
    print(type(t))

