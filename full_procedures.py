from time import sleep

import api_functions
import db_functions
from minor_modules import time_interval_sanitizer


@time_interval_sanitizer()
def time_series_full_save(symbol: str, market_identification_code: str, time_interval: str, verbose=False):
    """
    automates entire process of downloading the data and then saving it directly into database from source
    """
    if db_functions.time_series_table_exists(symbol, market_identification_code, time_interval=time_interval):
        data = api_functions.download_full_equity_history(
            symbol=symbol, mic_code=market_identification_code, verbose=verbose, time_interval=time_interval)
        db_functions.insert_equity_historical_data(
            data, equity_symbol=symbol, mic_code=market_identification_code, time_interval=time_interval)
    else:
        print("time series not created yet, creating new and downloading")
        db_functions.create_time_series(symbol, market_identification_code, time_interval=time_interval)
        time_series_full_save(symbol, market_identification_code, verbose=verbose, time_interval=time_interval)
        # raise db_helpers.TimeSeriesNotFoundError(f"Table {symbol}_{market_identification_code} "
        #                                          f"not present in time_series schema")


@time_interval_sanitizer()
def time_series_full_update(symbol: str, market_identification_code: str, time_interval: str):
    """
    update time series of given symbol/exchange pair. Use last record in database to determine the query size
    """
    if db_functions.time_series_table_exists(
            symbol, market_identification_code, time_interval=time_interval):
        pass


def perpare_database():
    """
    Set up the entire structure of database in correct order
    """


def fill_database():
    """
    Fill in empty database with basic information to make it ready-to-use. After this step, it should be able to
    make timeseries views, as well as time series saves/updates
    """
    forex_data: list[dict] = api_functions.get_all_currency_pairs('rapid', 'json')['data']
    currencies = set()
    currency_groups = set()

    # process downloaded forex data
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

    db_functions.insert_forex_currency_groups(currency_groups)
    db_functions.insert_currencies(currencies)
    db_functions.insert_forex_pairs_available(forex_data)

    # download stock exchanges data
    sleep(8)
    stock_markets_data: list[dict] = api_functions.get_all_exchanges(
        'rapid', 'json', additional_params={'show_plan': True})['data']

    plans = set()
    countries = set()
    timezones = set()

    # process stock markets data
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
    db_functions.insert_markets(forex_data)

    sleep(8)
    stocks_data: list[dict] = api_functions.get_all_equities('rapid', 'json')['data']

    equity_types = set()

    for e in stocks_data:
        equity_types.update((str(e['type']),))

    db_functions.insert_investment_types(equity_types)
    # Freetrailer Group AS - do not have country
    # Whoosh Holding PAO - do not have country
    db_functions.insert_stocks(forex_data)


def refresh_database():
    """prepare database as though it was a clean slate"""
    db_functions.purge_db_structure()
    db_functions.import_db_structure()


if __name__ == '__main__':
    # params = {
    #     "symbol": "NVDA",
    #     "market_identification_code": "XNGS",
    #     "time_interval": "1day",
    #     "verbose": True,
    # }
    # time_series_full_save(**params)
    refresh_database()
