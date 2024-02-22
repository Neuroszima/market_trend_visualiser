from math import ceil
from pprint import pprint
from time import time, sleep
from ast import literal_eval
from datetime import datetime
from typing import Literal, Generator

from api_functions.miscellaneous_api import parse_get_response_, api_key_switcher_
from minor_modules import time_interval_sanitizer

TIMESTAMP = dict[Literal['datetime', 'unix_time'], [str, int]]


@time_interval_sanitizer()
def obtain_earliest_timestamp_(
        symbol: str, api_key_pair: tuple, mic_code: str = None, exchange: str = None,
        time_interval: str = None, timezone: str = None, ask_stock: bool = True) -> TIMESTAMP:
    """ask api for the earliest datapoint of certain ticker in their database"""
    if not time_interval:
        time_interval = "1min"
    if ask_stock:
        if not mic_code:
            mic_code = "XNGS"
    querystring = {
        "interval": time_interval,
        "symbol": symbol,
        "mic_code": mic_code,
    }

    # following is nearly always optional
    if timezone:
        querystring['timezone'] = timezone
    if exchange:
        querystring['exchange'] = exchange

    result = parse_get_response_(
        querystring,
        request_type="earliest_timestamp",
        data_type="json",
        api_key_pair=api_key_pair
    )
    return result


@time_interval_sanitizer()
def download_time_series_(symbol: str, api_key_pair: tuple, exchange=None, mic_code=None, currency=None,
                          start_date: datetime = None, end_date: datetime = None, date: datetime = None,
                          points=5000, data_type=None, time_interval=None):
    """
    recover data from API by performing a single query, passing required parameters to the POST request body
    maximum number of data points (candles) a single query can yield is 5000
    one can switch the usage of the api from "RapidApi" provider, or calling TwelveData API directly,
    doubling on possible number of queries per day per email registration

    how does the date limitations work?

    if both dates "end" and "start" are fed, both will be inclusive.
    This means result will include the timestamps for both ends.

    For example: [symbol:NVDA; mic:XNGS; start:2022-03-22 11:20; end:2022-02-25 10:20]

    This will result in 1111 datapoints of data, in a "latest first" order. Watch out however for dates, during which
    stock market was closed

    If more than 5000 data points are required to fill the time period (for example 4 months of 1 minute data)
    there is no other way than to query more than 1 time and then use last row timestamp as "end_date"

    this way next query will result in serving the "latest timestamp" from the "end_date" and not, lets say, today

    :param date: exact date that we want data from
    :param start_date: if alone, will yield results from "start" all the way until current day
    :param end_date: if alone, will yield results up to the "end"
    """

    if not currency:
        currency = "USD"
    if not mic_code:
        mic_code = "XNGS"
    if not time_interval:
        time_interval = "1min"
    if not data_type:
        data_type = "json"
    querystring = {
        "currency": currency,
        "mic_code": mic_code,
        "symbol": symbol,
        "interval": time_interval,
        "format": data_type,
    }

    # following are only nice to have
    if start_date:
        querystring['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
    if end_date:
        querystring['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
    if date:
        querystring['date'] = date.strftime('%Y-%m-%d %H:%M:%S')
    if points:
        querystring['outputsize'] = points
    # following is less precise than passing the MIC of the exchange, use with care
    if exchange:
        querystring['exchange'] = exchange

    results = parse_get_response_(
        querystring_parameters=querystring,
        request_type="time_series",
        data_type=data_type,
        api_key_pair=api_key_pair,
    )

    return results


def preprocess_dates_(start_date: datetime | None, end_date: datetime | None):
    """replace missing hours in both dates, if not formed correctly"""

    if start_date:
        session_open = datetime(
            year=start_date.year, month=start_date.month, day=start_date.day,
            hour=9, minute=30
        )
        session_end = datetime(
            year=start_date.year, month=start_date.month, day=start_date.day,
            hour=15, minute=59
        )
        if not (session_open <= start_date <= session_end):
            start_date = session_open
    if end_date:
        session_open = datetime(
            year=end_date.year, month=end_date.month, day=end_date.day,
            hour=9, minute=30
        )
        session_end = datetime(
            year=end_date.year, month=end_date.month, day=end_date.day,
            hour=15, minute=59
        )
        if not (session_open <= end_date <= session_end):
            end_date = session_end
    return start_date, end_date


def calculate_iterations_(
        first_historical_point: datetime, time_interval: str,
        end_date: datetime | None = None, ask_stock: bool = True):
    """
    calculate how many trading days are there to download and then obtain number of iterations to get all of them
    number of downloads is overestimated on purpose

    since stock market has opening and close hours, we need different method to calculate iterations
    for forex market tickers
    """
    if not end_date:
        end_date = datetime.now()
    diff = end_date - first_historical_point
    days = (diff.days + 1) * 0.76  # a bit more than 5/7 days in a week

    if time_interval == '1day':
        return ceil(days / 4999) + 1

    # following branching will support more time intervals if future improvements will demand so, that's why
    # it looks goofy for now
    if ask_stock:
        if time_interval == '1min':
            return ceil(days * 390 / 4999) + 1
    else:
        if time_interval == "1min":
            return ceil(days * 1440 / 4999) + 1
    raise ValueError('time interval not suitable for calculating iterations')


@time_interval_sanitizer()
def download_market_ticker_history_(
        symbol: str, key_switcher: Generator, time_interval=None, mic_code=None,
        exchange=None, currency=None, verbose=False, start_date: datetime = None, end_date: datetime = None):
    """
    Automates the process of downloading entire history of the index, from the TwelveData provider
    queries until the last datapoint/timestamp has been reached, which it checks separately in a different API query

    full history is downloaded when no timestamp is passed in the function body.

    this method, internally, downloads data only in json format

    :param symbol: ticker symbol from the exchange
    :param key_switcher: generator object made from designated function form api_functions module
    :param time_interval: default "1min", time distance between datapoints
    :param exchange: default "NASDAQ", if not asking for currency pair
    :param mic_code: more precise version of 'exchange', default "XNGS" when not passed in
    :param currency: currency required to buy traded ticker, default USD when not passed in
    :param start_date: historically the farthest point of interest, default to "earliest timestamp" if not passed
    :param end_date: historically the latest point of interest, default 'today' if not passed
    :param verbose: print information about download progress

    """

    start_date, end_date = preprocess_dates_(start_date, end_date)

    if not time_interval:
        time_interval = "1min"

    ask_equity = "/" not in symbol
    if ask_equity:  # asking for equity information, otherwise asking for forex pair
        if not mic_code:
            mic_code = "XNGS"
        if not exchange:
            exchange = "NASDAQ"
        if not currency:
            currency = "USD"

    # no more need for sleep of 8 seconds and calculations. Simply invoke "next(key_switcher)" to get the
    # delay calculated automatically
    if not start_date:
        earliest_timestamp = obtain_earliest_timestamp_(
            symbol, time_interval=time_interval, mic_code=mic_code, api_key_pair=next(key_switcher))
    else:
        earliest_timestamp = start_date

    if time_interval == "1min":
        date_string = '%Y-%m-%d %H:%M:%S'
    elif time_interval == "1day":
        date_string = '%Y-%m-%d'
    else:
        raise ValueError("Improper argument for this API query. Possible intervals for this app: ('1min', '1day')")

    full_time_series = []
    if isinstance(earliest_timestamp, dict):
        # print("target timestamp", earliest_timestamp['datetime'], work_period_start)
        first_historical_point = datetime.strptime(earliest_timestamp['datetime'], date_string)
    elif isinstance(earliest_timestamp, datetime):
        # print("target timestamp", earliest_timestamp, work_period_start)
        first_historical_point = earliest_timestamp
    else:
        TypeError('earliest timestamp has wrong type, possible types are (datetime, dict[\'datetime\'][str])')

    download_params = {
        "symbol": symbol,
        "time_interval": time_interval,
        "mic_code": mic_code,
        "exchange": exchange,
        "currency": currency,
        # date params make it inefficient in terms of obtainable 5k data points per query
        "end_date": end_date,
    }
    if start_date:
        download_params['start_date'] = start_date

    iterations = calculate_iterations_(
        first_historical_point, time_interval=time_interval,
        end_date=end_date, ask_stock=ask_equity)
    for j in range(iterations):
        partial_data: dict = download_time_series_(**download_params, api_key_pair=next(key_switcher))

        # zeroth element in further queries would overlap and appear twice so its later truncated
        if j == 0:
            full_time_series.append(partial_data['values'][0])

        starting_record: dict = partial_data['values'][0]
        last_record: dict = partial_data['values'][-1]
        if verbose:
            print("len values = ", len(partial_data['values']))
            if len(full_time_series) > 0:
                print("last record tracked:", full_time_series[-1])
                print(f"start of the current batch: {starting_record}")
                print("last record of current batch:", last_record)
            else:
                print("first batch")
                print(f"start of the current batch: {starting_record}")
                print("last record of current batch:", last_record)

        if time_interval == "1min":
            conversion_string = '%Y-%m-%d %H:%M:%S'
        elif time_interval == "1day":
            conversion_string = '%Y-%m-%d'
        new_latest_time_period = datetime.strptime(last_record['datetime'], conversion_string)
        # if starting_record:
        #     end_period = dat
        full_time_series.extend(partial_data['values'][1:])

        if verbose:
            print("downloaded rows", len(partial_data['values']))
            print("extending with removing duplicate row (new start: ", partial_data['values'][1], ")")

        # check ending condition - data is downloaded backwards in time
        if new_latest_time_period == first_historical_point or len(partial_data['values']) < 4999:
            # print(len(partial_data['values']) < 4999)
            # print('end of download reached')
            break

        # print(f'next dump up until following date: {new_latest_time_period}')

        # params refresh after recent download
        download_params = {
            "symbol": symbol,
            "time_interval": time_interval,
            "exchange": exchange,
            "currency": currency,
            "end_date": new_latest_time_period,
        }
        if start_date:
            download_params['start_date'] = start_date

    # print(len(full_time_series))
    return full_time_series

