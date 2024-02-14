from time import sleep

import api_functions
import db_functions
from minor_modules import time_interval_sanitizer


@time_interval_sanitizer()
def time_series_save(
        symbol: str, market_identification_code: str, time_interval: str,
        verbose=False, api_key_permission_list: list | None = None):
    """
    automates entire process of downloading the data and then saving it directly into database from source
    """
    raise NotImplementedError("this function is not yet ready to use")
    # if db_functions.time_series_table_exists(symbol, market_identification_code, time_interval=time_interval):
    #     if db_functions.time_series_latest_timestamp():
    #         raise db_functions.TimeSeriesExists("this time series already has data, use another method to update it")
    #     data = api_functions.download_equity_history(
    #         symbol=symbol, mic_code=market_identification_code, verbose=verbose,
    #         time_interval=time_interval, api_key_permission_list=api_key_permission_list
    #     )
    #     db_functions.insert_equity_historical_data(
    #         data, equity_symbol=symbol, mic_code=market_identification_code, time_interval=time_interval)
    # else:
    #     if verbose:
    #         print("time series not created yet, creating new and downloading")
    #     db_functions.create_time_series(symbol, market_identification_code, time_interval=time_interval)
    #     time_series_save(symbol, market_identification_code, verbose=verbose, time_interval=time_interval)


@time_interval_sanitizer()
def time_series_full_update(
        symbol: str, market_identification_code: str, time_interval: str,
        api_key_permission_list: list | None = None):
    """
    update time series of given symbol/exchange_code pair. Use last record in database to determine the query size
    """
    raise NotImplementedError("this function is not yet ready to use")

    # if db_functions.time_series_table_exists(
    #         symbol, market_identification_code, time_interval=time_interval):
    #     latest_database_timestamp = db_functions.time_series_latest_timestamp()
    #
    #     return
    # raise db_functions.TimeSeriesNotFoundError(
    #     "this time series does not exist, use another function to create and populate it")


def perpare_database():
    """Set up the entire structure of database in correct order"""
    db_functions.import_db_structure()


def fill_database(api_key_permission_list: list | None = None):
    """
    Fill in empty database with basic information to make it ready-to-use. After this step, it should be able to
    make timeseries views, as well as time series saves/updates
    """
    # download all the necessary data
    key_switcher = api_functions.api_key_switcher(permitted_keys=api_key_permission_list)
    forex_data: list[dict] = api_functions.get_all_currency_pairs(next(key_switcher), 'json')['data']
    stock_markets_data: list[dict] = api_functions.get_all_exchanges(
        next(key_switcher), 'json', additional_params={'show_plan': True})['data']
    stocks_data: list[dict] = api_functions.get_all_equities(
        next(key_switcher), 'json', additional_params={'show_plan': True})['data']

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

    print(countries)
    # return

    db_functions.insert_timezones(timezones)
    db_functions.insert_countries(countries)
    db_functions.insert_plans(plans)
    db_functions.insert_markets(stock_markets_data)

    equity_types = set()

    for e in stocks_data:
        equity_types.update((str(e['type']),))

    db_functions.insert_investment_types(equity_types)
    # Freetrailer Group AS - do not have country
    # Whoosh Holding PAO - do not have country
    db_functions.insert_stocks(stocks_data)


def rebuild_database_destructively():
    """
    prepare database as though it was a clean slate

    THIS PURGES ENTIRE DATABASE!!! if you have something worth keeping there, better do a db backup first before
    invoking this function
    """
    db_functions.purge_db_structure()
    db_functions.import_db_structure()


if __name__ == '__main__':
    rebuild_database_destructively()
    fill_database()
    # params = {
    #     "symbol": "NVDA",
    #     "market_identification_code": "XNGS",
    #     "time_interval": "1day",
    #     "verbose": True,
    # }
    # time_series_full_save(**params)
