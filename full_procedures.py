
from api_functions import time_series_api
from db_functions import index_db, db_helpers


# TODO - unfinished
def time_series_full_save(symbol: str, market_identification_code: str, verbose=False):
    """
    automates entire process of downloading the data and then saving it directly into database from source
    """
    if db_helpers.time_series_table_exists(symbol, market_identification_code):
        data = time_series_api.download_full_index_history(
            symbol=symbol, mic_code=market_identification_code, verbose=verbose)
        index_db.insert_index_historical_data(
            data, equity_symbol=symbol, mic_code=market_identification_code)
    else:
        raise db_helpers.TimeSeriesNotFoundError(f"Table {symbol}_{market_identification_code} "
                                                 f"not present in time_series schema")


if __name__ == '__main__':
    params = {
        "symbol": "AAPL",
        "market_identification_code": "XNGS",
        "verbose": True
    }
    time_series_full_save(**params)
