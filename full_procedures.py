from datetime import datetime, timedelta
from typing import Generator
from warnings import warn

import api_functions
import db_functions
from minor_modules import time_interval_sanitizer


def exotic_markets_warning():
    warn("So far this function has not been tested with keys that are listed as 'paid' service. "
         "I.e. I do not know how the function will perform for symbols from exotic markets (like Indonesia)")


@time_interval_sanitizer()
def time_series_save(
        symbol: str, market_identification_code: str, time_interval: str, key_switcher: Generator, verbose=False):
    """
    automates entire process of downloading the data and then saving it directly into database from source
    """
    exotic_markets_warning()
    is_equity = db_functions.is_stock(symbol)
    if db_functions.time_series_table_exists(
            symbol, time_interval=time_interval, mic_code=market_identification_code, is_equity=is_equity):
        if db_functions.time_series_latest_timestamp(
                symbol, is_equity=is_equity, time_interval=time_interval, mic_code=market_identification_code):
            raise db_functions.TimeSeriesExistsError(
                "this time series already has data, use another method to update it")

        data = api_functions.download_market_ticker_history(
            symbol=symbol, mic_code=market_identification_code, verbose=verbose,
            time_interval=time_interval, key_switcher=key_switcher
        )
        db_functions.insert_historical_data(
            data, symbol=symbol, mic_code=market_identification_code, time_interval=time_interval,
            is_equity=is_equity
        )

    else:
        if verbose:
            print("time series not created yet, creating new and downloading")
        db_functions.create_time_series(
            symbol, time_interval=time_interval, mic_code=market_identification_code, is_equity=is_equity
        )
        time_series_save(
            symbol, market_identification_code, time_interval, key_switcher, verbose
        )


@time_interval_sanitizer()
def time_series_update(
        symbol: str, market_identification_code: str, time_interval: str, key_switcher: Generator,
        verbose: bool = False, end_date: datetime | None = None):
    """
    update time series of given symbol/exchange_code pair. Use last record in database to determine the query size
    """
    exotic_markets_warning()
    is_equity = db_functions.is_stock(symbol)
    if db_functions.time_series_table_exists(
            symbol, mic_code=market_identification_code, time_interval=time_interval, is_equity=is_equity):
        latest_database_timestamp = db_functions.time_series_latest_timestamp(
            symbol, time_interval, is_equity, market_identification_code)
        if end_date:
            if latest_database_timestamp > end_date:
                raise db_functions.TimeSeriesExistsError("this time series already covers this timestamp history")

        data = api_functions.download_market_ticker_history(
            symbol=symbol, key_switcher=key_switcher, mic_code=market_identification_code,
            start_date=latest_database_timestamp, end_date=end_date, verbose=verbose, time_interval=time_interval
        )
        if is_equity:
            schema_name = f"{time_interval}_time_series"
            table_name = f"{symbol}_{market_identification_code}"
        else:
            schema_name = "forex_time_series"
            symbol_ = "_".join(symbol.split("/")).upper()
            table_name = f"{symbol_}_{time_interval}"
        last_datapoint_id = db_functions.last_row_ID(schema_name, table_name)
        db_functions.insert_historical_data(
            data, symbol=symbol, mic_code=market_identification_code, time_interval=time_interval,
            is_equity=is_equity, rownum_start=last_datapoint_id+1
        )
        return

    raise db_functions.TimeSeriesNotFoundError(
        "this time series does not exist, use another function to create and populate it")


def perpare_database():
    """Set up the entire structure of database in correct order"""
    db_functions.import_db_structure()


def fill_database(key_switcher: Generator):
    """
    Fill in empty database with basic information to make it ready-to-use. After this step, it should be able to
    make timeseries views, as well as time series saves/updates
    """
    # download all the necessary data
    forex_data: list[dict] = api_functions.get_all_currency_pairs(next(key_switcher), 'json')
    stock_markets_data: list[dict] = api_functions.get_all_exchanges(next(key_switcher), 'json')
    stocks_data: list[dict] = api_functions.get_all_equities(next(key_switcher), 'json')

    # process downloaded forex data
    currencies = set()
    currency_groups = set()

    for forex_pair_data in forex_data:

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

    # following currencies are not included in forex pairs and has to be included manually
    currencies.update((str({'name': "South Africa cent", "symbol": "ZAC"}),))
    currencies.update((str({'name': "Israeli agora", "symbol": "ILA"}),))

    db_functions.insert_forex_currency_groups(currency_groups)
    db_functions.insert_currencies(currencies)
    db_functions.insert_forex_pairs_available(forex_data)

    # process stock markets data
    plans = set()
    countries = set()
    timezones = set()

    for e in stock_markets_data:
        access_obj = {
            'plan': e["access"]['plan'],
            "global": e["access"]['global'],
        }
        plans.update((str(access_obj),))
        countries.update((str(e['country']),))
        timezones.update((str(e['timezone']),))

    db_functions.insert_timezones(timezones)
    db_functions.insert_countries(countries)
    db_functions.insert_plans(plans)
    db_functions.insert_markets(stock_markets_data)

    equity_types = set()

    for e in stocks_data:
        equity_types.update((str(e['type']),))

    db_functions.insert_investment_types(equity_types)
    db_functions.insert_stocks(stocks_data)


def rebuild_database_destructively():
    """
    prepare database as though it was a clean slate

    THIS PURGES ENTIRE DATABASE!!! if you have something worth keeping there, better do a db backup first before
    invoking this function
    """
    db_functions.purge_db_structure()
    db_functions.import_db_structure()


def fetch_time_series(
        symbol: str, time_interval: str,  is_equity: bool = True, mic_code: str | None = None,
        start_date: datetime | None = None, end_date: datetime | None = None,
        time_span: timedelta | None = None):
    """
    get data from timetable in the database

    data can be obtained in two ways:
    - provide start and end dates - function will fetch data beginning with start date all the way through
    end date (both ends inclusive)
    """
    raise NotImplementedError("this function is not yet ready and is a stub")
    # if not db_functions.time_series_table_exists(symbol, time_interval, is_equity, mic_code):
    #     raise db_functions.TimeSeriesNotFoundError(
    #     "Series does not exist in the database, did you perform a download?")

