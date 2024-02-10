from ast import literal_eval
from datetime import datetime

import psycopg2

from db_functions.db_helpers import (
    _connection_dict,
    _information_schema_time_series_check,
    db_string_converter_
)
from minor_modules import time_interval_sanitizer


# create queries
_create_time_table = """
create table if not exists "{time_interval}_time_series"."{symbol}_{market_identification_code}" (
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

# drop queries
_drop_time_table = """
drop table if exists "{time_interval}_time_series"."{symbol}_{market_identification_code}";
"""

# insert queries
_query_insert_equity_data = """
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
_last_timetable_point = """
SELECT series.datetime FROM "{time_interval}_time_series"."{symbol}_{market_identification_code}" series 
ORDER BY series."ID" DESC LIMIT 1
"""


@time_interval_sanitizer()
def insert_equity_historical_data_(
        historical_data: list[dict], equity_symbol: str, mic_code: str, time_interval: str,
        rownum_start: int = 0
    ):
    """
    Insert historical data for given equity into the database. Earliest timestamped rows are inserted first
    rownum_start is used, when table already has some data, and user wants to append fresh changes to time series
    """

    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        if datetime.strptime(literal_eval(historical_data[0]['datetime']), '%Y-%m-%d %H:%M:%S') > \
            datetime.strptime(literal_eval(historical_data[-1]['datetime']), '%Y-%m-%d %H:%M:%S'):
            historical_data = reversed(historical_data)
        #  iterate from oldest to newest - new rows will be appended to the farthest row anyway
        for rownum, candle in enumerate(historical_data):
            print(candle)
            query_dict = {
                "index": rownum + rownum_start,
                "equity_symbol": equity_symbol,
                "time_series_schema": f'"{time_interval}_time_series"',
                "market_identification_code": mic_code,
                "timestamp": f"{candle['datetime']}",
                "open_price": candle['open'],
                "close_price": candle['close'],
                "high_price": candle['high'],
                "low_price": candle['low'],
                "volume": candle['volume'],
            }
            cur.execute(_query_insert_equity_data.format(**query_dict))
            conn.commit()
        cur.close()


@time_interval_sanitizer()
def time_series_table_exists_(symbol: str, market_identification_code: str, time_interval: str) -> bool:
    """
    check if the given table exists in the time-specific schema
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        table_name = db_string_converter_(f"{symbol}_{market_identification_code}")
        time_series_schema = f'{time_interval}_time_series'
        cur.execute(_information_schema_time_series_check.format(
            table_name=table_name,
            time_series_schema=db_string_converter_(time_series_schema),
        ))
        result = cur.fetchall()
        if result:
            return True
        return False


@time_interval_sanitizer()
def create_time_series_(symbol: str, market_identification_code: str, time_interval: str) -> None:
    """
    creates table inside database schema, that corresponds to time_interval passed into function call

    each time interval has corresponding database schema that saves stock market price history
    for the given symbol/MIC pair
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        q_dict = {
            "market_identification_code": market_identification_code,
            "symbol": symbol,
            "lower_symbol": symbol.lower(),
            "time_interval": time_interval,
        }
        cur.execute(_create_time_table.format(**q_dict))
    assert time_series_table_exists_(symbol, market_identification_code, time_interval)


@time_interval_sanitizer()
def time_series_latest_timestamp_(symbol: str, market_identification_code: str, time_interval: str) -> datetime | None:
    """
    grab the latest datapoint from a timeseries and extract the data from it in the format ready to be
    consumed by API loader
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        q_dict = {
            "market_identification_code": market_identification_code,
            "symbol": symbol,
            "time_interval": time_interval,
        }
        cur.execute(
            _last_timetable_point.format(**q_dict))
        last_record = cur.fetchall()
        try:
            t_ = last_record[0][0]  # this will already be a "datetime.datetime()" python object
        except IndexError:
            t_ = None
        return t_


if __name__ == '__main__':
    stock_, market_identification_code_ = "OTEX", "XNGS"
    interval_ = "1min"

    # clean for this mini-test
    with psycopg2.connect(**_connection_dict) as conn:
        cur_ = conn.cursor()
        cur_.execute(_drop_time_table.format(
            time_interval=interval_,
            symbol=stock_,
            market_identification_code=market_identification_code_
        ))
    create_time_series_(
        symbol=stock_, market_identification_code=market_identification_code_, time_interval=interval_)

    # populate table with dummy data
    historical_dummy_data = [
        {
            "index": 2, "equity_symbol": stock_,
            "time_series_schema": f'"{interval_}_time_series"',
            "market_identification_code": market_identification_code_,
            "datetime": f"'2020-03-24 09:37:00'",
            "open": 3, "close": 5, "high": 8, "low": 1.50,
            "volume": 490,
        },
        {
            "index": 1, "equity_symbol": stock_,
            "time_series_schema": f'"{interval_}_time_series"',
            "market_identification_code": market_identification_code_,
            "datetime": f"'2020-03-24 09:36:00'",
            "open": 1, "close": 3, "high": 4, "low": 0.50,
            "volume": 190,
        }
    ]
    insert_equity_historical_data_(
        historical_dummy_data, equity_symbol=stock_, mic_code=market_identification_code_, time_interval=interval_
    )
    insert_equity_historical_data_(
        historical_dummy_data, equity_symbol=stock_, mic_code=market_identification_code_, time_interval=interval_,
        rownum_start=2,
    )

    assert time_series_table_exists_(stock_, market_identification_code_, interval_)
    t = time_series_latest_timestamp_(stock_, market_identification_code_, interval_)
    print(t)
    print(type(t))

    # restore for other mini-tests
    with psycopg2.connect(**_connection_dict) as conn:
        cur_ = conn.cursor()
        cur_.execute(_drop_time_table.format(
            time_interval=interval_,
            symbol=stock_,
            market_identification_code=market_identification_code_
        ))
    create_time_series_(
        symbol=stock_, market_identification_code=market_identification_code_, time_interval=interval_)
