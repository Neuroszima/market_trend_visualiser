from time import time, sleep
from ast import literal_eval
from datetime import datetime  # planned to use timedelta but not useful

from api_functions.miscellaneous_api import parse_get_response_
from minor_modules import time_interval_sanitizer
# from minor_modules.helpers import time_interval_sanitizer


@time_interval_sanitizer()
def obtain_earliest_timestamp_(
        symbol: str, mic_code: str = None, exchange: str = None,
        time_interval: str = None, timezone: str = None, api_type: str = None):
    if not time_interval:
        time_interval = "1min"
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
        api_type=api_type
    )
    return result


@time_interval_sanitizer()
def download_time_series_(symbol: str, exchange=None, mic_code=None, currency=None,
                          start_date: datetime = None, end_date: datetime = None, date: datetime = None,
                          points=5000, data_type=None, time_interval=None, api_type=None):
    """
    recover data from API by performing a single query, passing required parameters to the POST request body
    maximum number of data points (candles) a single query can yield is 5000
    one can switch the usage of the api from "RapidApi" provider, or calling TwelveData API directly,
    doubling on possible number of queries per day per email registration
    """

    if api_type is None:
        api_type = "rapid"
    if not currency:
        currency = "USD"
    if not mic_code:
        mic_code = "XNGS"
    # if not exchange:
    #     exchange = "NASDAQ"
    if not time_interval:
        time_interval = "1min"
    if not data_type:
        data_type = "CSV"
    querystring = {
        "currency": currency,
        # "exchange": exchange,
        "mic_code": mic_code,
        "symbol": symbol,
        "interval": time_interval,
        "format": data_type,
    }

    # following are only nice to have
    if start_date:
        querystring['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
    if end_date:
        # "end_date": "2023-05-11 11:35:00"
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
        api_type=api_type,
    )

    return results


@time_interval_sanitizer()
def download_full_equity_history_(
        symbol: str, time_interval=None, mic_code=None, exchange=None, currency=None, verbose=False):
    """
    Automates the process of downloading entire history of the index, from the TwelveData provider
    queries until the last datapoint/timestamp has been reached, which it checks separately in a different API query
    """
    print(f"{time_interval=}")
    if not time_interval:
        time_interval = "1min"
    if not mic_code:
        mic_code = "XNGS"
    if not exchange:
        exchange = "NASDAQ"
    if not currency:
        currency = "USD"

    # decide which api to use first and start tracking api usage
    api_type = "rapid"
    rapid_used = False
    regular_used = False

    # we need to sleep 8 seconds after this - API provider uses averaging and too many
    # requests in short time will mean "no more tries for you pal"
    # In other words - even if you do "counting per minute",
    # depleting all the tokens in first few seconds of that minute will result in an error on another read
    # if proceeded too fast, even after clocked 60 seconds pass
    earliest_timestamp = obtain_earliest_timestamp_(symbol, api_type=api_type, time_interval=time_interval)
    sleep(8)

    work_period_start = time()
    full_time_series = []
    print("target timestamp", earliest_timestamp['datetime'], work_period_start)

    if time_interval == "1min":
        first_historical_point = datetime.strptime(earliest_timestamp['datetime'], '%Y-%m-%d %H:%M:%S')
    elif time_interval == "1day":
        first_historical_point = datetime.strptime(earliest_timestamp['datetime'], '%Y-%m-%d')
    else:
        raise ValueError("Improper argument for this API query. Possible intervals for this app: ('1min', '1day')")

    download_params = {
        "symbol": symbol,
        "time_interval": time_interval,
        "mic_code": mic_code,
        "exchange": exchange,
        "currency": currency,
        # date params make it inefficient in terms of obtainable 5k data points per query
        "end_date": None,
        "data_type": "json",
    }

    # following loop, when invoked on "1min" timeseries can download about 5 years worth of historical data
    # at the moment of making this (start of 2024) this is enough.
    for j in range(150):  # limited number of repeats just for safety - ~748k points potential anyway...
        partial_data = download_time_series_(**download_params, api_type=api_type)

        if isinstance(partial_data, str):
            d = literal_eval(partial_data)
        else:
            d = partial_data

        try:
            starting_record = d['values'][0]
        except KeyError as e:
            print(e)
            print(d)
            raise e
        last_record = d['values'][-1]
        if verbose:
            if len(full_time_series) > 0:
                print("last record tracked:", full_time_series[-1])
                print(f"start of the current batch: {starting_record}")
                print("last record of current batch:", last_record)
            else:
                print("first batch")
                print(f"start of the current batch: {starting_record}")
                print("last record of current batch:", last_record)

        if time_interval == "1min":
            time_period = datetime.strptime(last_record['datetime'], '%Y-%m-%d %H:%M:%S')
        elif time_interval == "1day":
            time_period = datetime.strptime(last_record['datetime'], '%Y-%m-%d')
        full_time_series.extend(d['values'][1:])

        if verbose:
            print("downloaded rows", len(d['values']))
            print("extending with removing duplicate row (new start: ", d['values'][1:][0], ")")

        # check ending condition
        if time_period == first_historical_point:
            print('end of download reached')
            break

        print(f'next dump up until following date: {time_period}')

        # not finished yet? keep api usage in check to not get any errors in output
        if api_type == "rapid":
            rapid_used = True
        elif api_type == "regular":
            regular_used = True
        print("api usage count:", (rapid_used, regular_used))

        if rapid_used:
            print("api type switch")
            api_type = "regular"

        if rapid_used and regular_used:
            print("api tokens depleted, switching back to rapid and sleep")
            api_type = "rapid"
            sleep(7.4)
            rapid_used = False
            regular_used = False

        # params refresh after recent download
        download_params = {
            "symbol": symbol,
            "time_interval": time_interval,
            "exchange": exchange,
            "currency": currency,
            "end_date": time_period,
            # using those, is not needed
            # "end_date": time_period+timedelta(days=20),
            # "start_date": time_period,
            # "date": time_period,
            "data_type": "json",
        }

    print(len(full_time_series))
    return full_time_series


if __name__ == '__main__':
    stock = "NVDA"
    # following is ~250k rows download
    # time_series = download_full_equity_history(symbol=stock)
    # with open('../db_functions/otex_series_OG.txt', 'w', encoding="UTF-8", newline='\n') as api_test:
    #     for entry in time_series:
    #         print(type(entry))
    #         api_test.write(str(entry) + "\n")
    print(obtain_earliest_timestamp_(stock, mic_code="XNGS", time_interval='1day', api_type='rapid'))
    # sleep(8)
    # print(obtain_earliest_timestamp(stock, mic_code="XNGS", time_interval='1min'))
