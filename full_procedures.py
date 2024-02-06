from api_functions import time_series_api
from db_functions import index_db, db_helpers, time_series_db
from minor_modules.helpers import time_interval_sanitizer


@time_interval_sanitizer()
def time_series_full_save(symbol: str, market_identification_code: str, time_interval: str, verbose=False):
    """
    automates entire process of downloading the data and then saving it directly into database from source
    """
    if time_series_db.time_series_table_exists(symbol, market_identification_code, time_interval=time_interval):
        data = time_series_api.download_full_equity_history(
            symbol=symbol, mic_code=market_identification_code, verbose=verbose, time_interval=time_interval)
        index_db.insert_equity_historical_data(
            data, equity_symbol=symbol, mic_code=market_identification_code, time_interval=time_interval)
    else:
        print("time series not created yet, creating new and downloading")
        time_series_db.create_time_series(symbol, market_identification_code, time_interval=time_interval)
        time_series_full_save(symbol, market_identification_code, verbose=verbose, time_interval=time_interval)
        # raise db_helpers.TimeSeriesNotFoundError(f"Table {symbol}_{market_identification_code} "
        #                                          f"not present in time_series schema")


@time_interval_sanitizer()
def time_series_full_update(symbol: str, market_identification_code: str, time_interval: str):
    """
    update time series of given symbol/exchange pair. Use last record in database to determine the query size
    """
    if time_series_db.time_series_table_exists(symbol, market_identification_code, time_interval=time_interval):
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


if __name__ == '__main__':
    params = {
        "symbol": "NVDA",
        "market_identification_code": "XNGS",
        "time_interval": "1day",
        "verbose": True,
    }
    time_series_full_save(**params)
