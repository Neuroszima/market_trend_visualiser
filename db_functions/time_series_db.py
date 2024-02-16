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
_create_forex_table = """
create table if not exists "forex_time_series"."{symbol}_{time_interval}" (
    "ID" integer not null,
    datetime timestamp without time zone,
    open numeric(10,5),
    close numeric(10,5),
    high numeric(10,5),
    low numeric(10,5),
    constraint {lower_symbol}_time_series_pkey primary key("ID")
);
"""

# drop queries
_drop_time_table = """
drop table if exists "{time_interval}_time_series"."{symbol}_{market_identification_code}";
"""
_drop_forex_table = """
drop table if exists "forex_time_series"."{symbol}_{time_interval}";
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
"""
_query_insert_forex_data = """
INSERT INTO "forex_time_series".\"{symbol}_{time_interval}\" 
(\"ID\", datetime, open, close, high, low) 
VALUES (
    {index},
    {timestamp}:: timestamp without time zone,
    {open_price},
    {close_price},
    {high_price},
    {low_price}
);
"""  # the ":: timestamp.." is comment in PSQL, so it's ok

# select queries
_last_equity_timetable_point = """
SELECT series.datetime FROM "{time_interval}_time_series"."{symbol}_{market_identification_code}" series 
ORDER BY series."ID" DESC LIMIT 1
"""
_last_forex_timetable_point = """
SELECT series.datetime FROM "forex_time_series"."{symbol}_{time_interval}" series 
ORDER BY series."ID" DESC LIMIT 1
"""


@time_interval_sanitizer()
def insert_equity_historical_data_(
        historical_data: list[dict], symbol: str, time_interval: str,
        rownum_start: int = 0, is_equity=True, mic_code: str | None = None
):
    """
    Insert historical data for given equity into the database. Earliest timestamped rows are inserted first
    rownum_start is used, when table already has some data, and user wants to append fresh changes to time series

    :param is_equity: differentiates from forex pairs and equity (stock/bond/etc.) time series
    :param mic_code: if inserting equity data, use it to denote exchange from which it comes
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
                "open_price": candle['open'],
                "close_price": candle['close'],
                "high_price": candle['high'],
                "low_price": candle['low'],
                "timestamp": f"{candle['datetime']}"
            }
            if is_equity:
                query_dict["equity_symbol"] = symbol
                query_dict["time_series_schema"] = f'"{time_interval}_time_series"'
                query_dict["market_identification_code"] = mic_code
                query_dict["volume"] = candle['volume']
                cur.execute(_query_insert_equity_data.format(**query_dict))
            else:
                query_dict['symbol'] = "_".join(symbol.split("/")).upper()
                query_dict['time_interval'] = time_interval
                cur.execute(_query_insert_forex_data.format(**query_dict))
            conn.commit()
        cur.close()


@time_interval_sanitizer()
def time_series_table_exists_(symbol: str, time_interval: str, is_equity=True, mic_code: str | None = None) -> bool:
    """
    check if the given table exists in the time-specific schema
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        if is_equity:
            table_name = db_string_converter_(f"{symbol}_{mic_code}")
            time_series_schema = f'{time_interval}_time_series'
        else:
            symbol_ = "_".join(symbol.split("/")).upper()  # fixing some small characters like in "GBp"
            table_name = db_string_converter_(f"{symbol_}_{time_interval}")
            time_series_schema = "forex_time_series"

        print(_information_schema_time_series_check.format(
            table_name=table_name,
            time_series_schema=db_string_converter_(time_series_schema),
        ))
        cur.execute(_information_schema_time_series_check.format(
            table_name=table_name,
            time_series_schema=db_string_converter_(time_series_schema),
        ))
        result = cur.fetchall()
        if result:
            return True
        return False


@time_interval_sanitizer()
def create_time_series_(symbol: str, time_interval: str, is_equity=True, mic_code: str | None = None) -> None:
    """
    creates table inside database schema, that corresponds to time_interval passed into function call

    each time interval has corresponding database schema that saves stock market price history
    for the given symbol/MIC pair
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        if is_equity:
            q_dict = {
                "market_identification_code": mic_code,
                "symbol": symbol,
                "lower_symbol": symbol.lower(),
                "time_interval": time_interval,
            }
            cur.execute(_create_time_table.format(**q_dict))
        else:
            symbol_ = "_".join(symbol.split("/")).upper()  # fixing some small characters like in "GBp"
            q_dict = {
                "symbol": symbol_,
                "lower_symbol": symbol_.lower(),
                "time_interval": time_interval,
            }
            cur.execute(_create_forex_table.format(**q_dict))
    assert time_series_table_exists_(symbol, time_interval, is_equity=is_equity, mic_code=mic_code)


@time_interval_sanitizer()
def time_series_latest_timestamp_(
        symbol: str, time_interval: str, is_equity: bool = True, mic_code: str | None = None) -> datetime | None:
    """
    grab the latest datapoint from a timeseries and extract the data from it in the format ready to be
    consumed by API loader
    """
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()

        q_dict = {
            "time_interval": time_interval,
        }
        if is_equity:
            q_dict["symbol"] = symbol
            q_dict["market_identification_code"] = mic_code
            cur.execute(_last_equity_timetable_point.format(**q_dict))
        else:
            q_dict["symbol"] = "_".join(symbol.split("/")).upper()
            cur.execute(_last_forex_timetable_point.format(**q_dict))
        last_record = cur.fetchall()
        try:
            t_ = last_record[0][0]  # this will already be a "datetime.datetime()" python object
        except IndexError:
            t_ = None
        return t_

