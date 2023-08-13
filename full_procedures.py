from api_functions import time_series_api
from db_functions import index_db, db_helpers, time_series_db


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


if __name__ == '__main__':
    params = {
        "symbol": "NVDA",
        "market_identification_code": "XNGS",
        "time_interval": "1day",
        "verbose": True,
    }
    time_series_full_save(**params)
