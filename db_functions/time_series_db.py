from datetime import datetime, timedelta
from typing import Literal

import psycopg2
from psycopg2.errors import UndefinedTable

from db_functions.db_helpers import (
    is_equity_, is_forex_pair_,
    _connection_dict,
    _information_schema_table_check,
    db_string_converter_,
    TimeSeriesNotFoundError_,
    DataNotPresentError_,
    DataUncertainError_
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
    volume bigint,
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
    constraint {lower_symbol}_{time_interval}_time_series_pkey primary key("ID")
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
_last_timetable_point = """
SELECT series.datetime FROM "{time_series_schema}"."{time_series_table}" series 
ORDER BY series."ID" DESC LIMIT 1
"""

_query_get_data_from_equity_timeseries = """
SELECT * FROM "{time_interval}_time_series"."{symbol}_{market_identification_code}";
"""
_query_get_data_from_forex_timeseries = """
SELECT * FROM "forex_time_series"."{symbol}_{time_interval}";
"""
_query_get_single_timeseries_point = """
SELECT * FROM "{time_series_schema}"."{time_series_table}" series 
where series.datetime = '{search_date}';
"""
_query_get_ID_from_table_by_date = """
SELECT "ID" FROM "{schema_name}"."{table_name}" series
where series.datetime {operation} TIMESTAMP '{search_date}' ORDER BY series.datetime {operation_order} LIMIT 1;
"""
_query_get_data_by_timestamps = """
SELECT * FROM \"{schema_name}\".\"{table_name}\" series 
WHERE series.datetime {earlier_bracket} {optional_and} {later_bracket} {optional_limit};
"""


@time_interval_sanitizer()
def resolve_time_series_location_(
    symbol: str, time_interval: str, is_equity: bool | None = None,
        mic_code: str | None = None) -> tuple[str, str, bool]:
    """check if data provided actually points to a table existing in database or not"""
    if not isinstance(is_equity, bool):
        is_equity = is_equity_(symbol)
        is_forex_pair = is_forex_pair_(symbol)
    else:
        is_forex_pair = not is_equity

    if is_equity:
        if mic_code is None:
            raise ValueError('market identifier code should be passed along the ticker')
        schema_name = f"{time_interval}_time_series"
        table_name = f"{symbol}_{mic_code}"
        is_equity = True
    elif is_forex_pair:
        if mic_code is not None:
            raise ValueError('market identifier should be skipped when passing symbol that resolves to forex pair')
        schema_name = "forex_time_series"
        base, quote = symbol.split("/")
        table_name = f"{base}_{quote}_{time_interval}"
        is_equity = False
    else:
        raise DataUncertainError_("with current information, couldn't find the data "
                                  f"resembling anything that is in the database: {symbol}")
    return schema_name, table_name, is_equity


def insert_historical_data_(
        historical_data: list[dict], symbol: str, time_interval: str,
        rownum_start: int = 0, is_equity: bool | None = None, mic_code: str | None = None):
    """
    Insert historical data for given equity into the database. Earliest timestamped rows are inserted first
    rownum_start is used, when table already has some data, and user wants to append fresh changes to time series
    for this function, the 'is_equity' has been left alone with 'True/False' to make it easier to

    :param is_equity: differentiates from forex pairs and equity (stock/bond/etc.) time series
    :param mic_code: if inserting equity data, use it to denote exchange from which it comes
    """
    _, __, is_equity = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)

    if time_interval in ['1day']:  # future-thinking about other time intervals allowed by provider...
        timestring = '%Y-%m-%d'
    elif time_interval in ['1min']:  # ~||~ (^ as above)
        timestring = '%Y-%m-%d %H:%M:%S'
    with psycopg2.connect(**_connection_dict) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        zero_timestamp: str = historical_data[0]['datetime']
        last_timestamp: str = historical_data[-1]['datetime']
        if datetime.strptime(zero_timestamp, timestring) > datetime.strptime(last_timestamp, timestring):
            historical_data = reversed(historical_data)
        #  iterate from oldest to newest - new rows will be appended to the farthest row anyway
        for rownum, candle in enumerate(historical_data):
            query_dict = {
                "index": rownum + rownum_start,
                "open_price": candle['open'],
                "close_price": candle['close'],
                "high_price": candle['high'],
                "low_price": candle['low'],
                "timestamp": f"'{candle['datetime']}'"
            }
            if is_equity:
                query_dict["equity_symbol"] = symbol
                query_dict["time_series_schema"] = f'"{time_interval}_time_series"'
                query_dict["market_identification_code"] = mic_code
                query_dict["volume"] = candle['volume']
                try:
                    cur.execute(_query_insert_equity_data.format(**query_dict))
                except psycopg2.Error as e:
                    print(e)
                    print(query_dict)
                    raise psycopg2.Error("there was error with database")
            else:
                query_dict['symbol'] = "_".join(symbol.split("/")).upper()
                query_dict['time_interval'] = time_interval
                cur.execute(_query_insert_forex_data.format(**query_dict))
        conn.commit()
        cur.close()


def time_series_table_exists_(
        symbol: str, time_interval: str, is_equity: bool | None = None, mic_code: str | None = None) -> bool:
    """
    check if the given table exists in the time-specific schema
    """
    time_series_schema, table_name, _ = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(_information_schema_table_check.format(
            table_name=db_string_converter_(table_name),
            schema=db_string_converter_(time_series_schema),
        ))
        result = cur.fetchall()
        if result:
            return True
        return False


def create_time_series_(
        symbol: str, time_interval: str, is_equity: bool | None = None, mic_code: str | None = None) -> None:
    """
    creates table inside database schema, that corresponds to time_interval passed into function call

    each time interval has corresponding database schema that saves stock market price history
    for the given symbol/MIC pair
    """
    # retrieve schema and table names
    schema_name, table_name, is_equity = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)
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


def time_series_latest_timestamp_(
        symbol: str, time_interval: str, is_equity: bool | None = None, mic_code: str | None = None) -> datetime | None:
    """
    grab the latest datapoint from a timeseries and extract the data from it in the format ready to be
    consumed by API loader
    """
    # retrieve schema and table names
    schema_name, table_name, is_equity = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)

    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        q_dict = {
            "time_series_schema": schema_name,
            "time_series_table": table_name,
        }
        cur.execute(_last_timetable_point.format(**q_dict))
        last_record = cur.fetchall()
        try:
            t_ = last_record[0][0]  # this will already be a "datetime.datetime()" python object
        except IndexError:
            t_ = None
    return t_


def fetch_datapoint_by_date_(
        date: datetime | str, symbol: str, time_interval: str,
        is_equity: bool | None = None, mic_code: str | None = None):
    """
    Obtain a point from database based on timestamp
    For now we require absolute precision in terms of selecting dates. If you fail to pass a date,
    that already has a point associated with it, this function will raise.
    """
    # retrieve schema and table names
    schema_name, table_name, is_equity = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)

    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        q = _query_get_single_timeseries_point.format(
            time_series_schema=schema_name,
            time_series_table=table_name,
            search_date=date,
        )
        try:
            cur.execute(q)
        # user forced 'False/True' in 'is_equity' and PSQL didn't finc anything despite efforts
        except UndefinedTable:
            raise TimeSeriesNotFoundError_(f'There is no time series: {schema_name}.{table_name}')
        res = cur.fetchall()
    if not res:
        raise DataNotPresentError_(f"There is no point in data that is associated with date: {date}")
    if len(res) > 1:
        raise DataUncertainError_("there is more than one point with that date in timeseries!")
    return res[0]


def fetch_ID_closest_to_date_(
        date_to_check: datetime, operation: Literal['<=', '>='],
        symbol: str, time_interval: str, is_equity: bool | None = None, mic_code: str | None = None) -> int:
    """search for the closest datapoint to the date given as argument"""
    if operation not in ['<=', '>=']:
        raise ValueError(f'Operation {operation} is not allowed. allowed operations: "<=", "=>"')
    schema_name, table_name, _ = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)
    with psycopg2.connect(**_connection_dict) as conn:
        cur = conn.cursor()
        q = {
            "schema_name": schema_name,
            "table_name": table_name,
            "operation": operation,
            "search_date": str(date_to_check),
            "operation_order": "ASC" if operation == ">=" else "DESC"
        }
        cur.execute(_query_get_ID_from_table_by_date.format(**q))
        res = cur.fetchall()
    if not res:
        raise DataNotPresentError_(
            f"Couldn't find datapoint closest to {date_to_check} "
            f'parameters: {schema_name=}, {table_name=}, {operation=}'
        )
    return res[0][0]


def calculate_fetch_time_bracket_(
        # schema_name: str, table_name: str,
        symbol: str, time_interval: str, is_equity: bool | None = None, mic_code: str | None = None,
        start_date: datetime | None = None, end_date: datetime | None = None,
        time_span: timedelta | None = None, trading_time_span: int | None = None):
    """
    Calculates what should be the start and end row ID range for fetch function to work properly

    Symbol, interval, is_equity and mic_code determine table to perform a search and optimizations.
    dates and time_span determine the approximate number of datapoints to fetch (by performing ID lookup)

    Difference between 'time_span' and 'trading_timespan' is as follows. Example takes "90 day lookup, 1-day timeframe".
    Former will look up at for example 3 months of trading, and it will be less than 90 datapoints
    because of Saturdays/Sundays/holidays, while latter will yield 90 trading days worth of data (up to today of course)

    If you provide multiple parameters, start_date and end_date will take precedence over passed time spans
    Please save yourself a minute and don't use this function to actually fetch data XD.
    """
    # decide if it is possible to form a bracket
    missing_count = [
        not start_date, not end_date,
        (not time_span) and (not trading_time_span)  # this one is kind of 'either-or'
    ].count(True)
    if missing_count > 1:
        raise LookupError(
            "At least 2 different parameters need to be passed to form a db lookup range, "
            "that can be used to perform a database fetch. \nChoose one configuration:"
            "(Two dates 'start-end'; One date, one interval)"
        )

    # retrieve schema and table names
    schema_name, table_name, is_equity = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)

    # calculate start and end of bracket, if datetime/timedelta was passed
    if start_date and time_span and (end_date is None):
        end_date = start_date + time_span
    elif end_date and time_span and (start_date is None):
        start_date = end_date - time_span

    earliest_id, latest_id = None, None
    # find ID of the item that is associated with the earliest possible datapoint closest to start_date
    if start_date:
        earliest_id = fetch_ID_closest_to_date_(
            start_date, operation=">=", symbol=symbol, time_interval=time_interval, mic_code=mic_code)
    # find ID of the item that is associated with the latest possible datapoint closest to end_date
    if end_date:
        latest_id = fetch_ID_closest_to_date_(
            end_date, operation="<=", symbol=symbol, time_interval=time_interval, mic_code=mic_code)

    if (latest_id is None) and (trading_time_span is not None) and (earliest_id is not None):
        latest_id = earliest_id + trading_time_span - 1
    if (earliest_id is None) and (trading_time_span is not None) and (latest_id is not None):
        earliest_id = latest_id - trading_time_span + 1

    if earliest_id < 0:
        earliest_id = 0

    # return both data ID's as (earliest, latest) tuple
    assert earliest_id is not None and latest_id is not None, \
        f"something went wrong with creating range: {earliest_id}, {latest_id}"
    return earliest_id, latest_id


def fetch_data_by_dates_(
        symbol: str, time_interval: str, is_equity: bool | None = None, mic_code: str | None = None,
        start_date: datetime | None = None, end_date: datetime | None = None,
        time_span: timedelta | None = None, trading_time_span: int | None = None):
    """
    Get data from database based on timestamps, or additional information
    Since this method only uses dates and its derivatives as an input, it is limited to query schemas containing
    time series information
    """
    # decide if it is possible to form a bracket
    missing_count = [
        not start_date, not end_date,
        (not time_span) and (not trading_time_span)  # this one is kind of 'either-or'
    ].count(True)
    if missing_count > 1:
        raise LookupError(
            "At least 2 different parameters need to be passed to form a db lookup range, "
            "that can be used to perform a database fetch. \nChoose one configuration:"
            "(Two dates 'start-end'; One date, one interval)"
        )

    # retrieve schema and table names
    schema_name, table_name, is_equity = resolve_time_series_location_(symbol, time_interval, is_equity, mic_code)

    # calculate start and end of bracket, if datetime/timedelta was passed
    if start_date and time_span and (end_date is None):
        end_date = start_date + time_span
    elif end_date and time_span and (start_date is None):
        start_date = end_date - time_span

    # optional number of datapoints
    if trading_time_span:
        optional_limit = f"LIMIT {trading_time_span}"
    else:
        optional_limit = ""

    if start_date and end_date:
        q = {
            "earlier_bracket": f">= TIMESTAMP '{start_date}'",
            "optional_and": "AND",
            "later_bracket": f"<= TIMESTAMP '{end_date}'",
        }
    elif end_date is not None and trading_time_span is not None:
        q = {
            "earlier_bracket": "",
            "optional_and": "",
            "later_bracket": f"<= TIMESTAMP '{end_date}'",
        }
    elif start_date is not None and trading_time_span is not None:
        q = {
            "earlier_bracket": f">= TIMESTAMP '{start_date}'",
            "optional_and": "",
            "later_bracket": "",
        }
    else:
        d = (schema_name, table_name, start_date, end_date, time_span, trading_time_span)
        raise RuntimeError(
            f'something went wrong, couldn\'t formulate a bracket even when data has been passed to function: {d}')

    q["schema_name"] = schema_name
    q["table_name"] = table_name
    q["optional_limit"] = optional_limit
    try:
        with psycopg2.connect(**_connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(_query_get_data_by_timestamps.format(**q))
            data = cur.fetchall()
    except UndefinedTable:
        raise TimeSeriesNotFoundError_(f'series: {schema_name}.{table_name} does not exist')
    return data
