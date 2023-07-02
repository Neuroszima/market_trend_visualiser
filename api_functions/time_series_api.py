from time import time, sleep
from typing import Optional
from ast import literal_eval
from datetime import datetime  # planned to use timedelta but not useful

from api_functions.miscellaneous_api import recover_get_response


def obtain_earliest_timestamp(
        index_symbol: str, exchange=None, interval=None, timezone=None, api_type: Optional[str] = None):
    if not interval:
        interval = "1min"
    if not exchange:
        exchange = "NASDAQ"
    querystring = {
        "interval": interval,
        "exchange": exchange,
        "symbol": index_symbol,
    }

    # following is nearly always optional
    if timezone:
        querystring['timezone'] = timezone

    result = recover_get_response(
        querystring,
        request_type="earliest_timestamp",
        data_type="json",
        api_type=api_type
    )
    return result


def download_time_series(index_symbol: str, exchange=None, currency=None,
                         start_date: datetime = None, end_date: datetime = None, date: datetime = None,
                         points=5000, data_type=None, interval=None, api_type=None):
    if api_type is None:
        api_type = "rapid"
    if not currency:
        currency = "USD"
    if not exchange:
        exchange = "NASDAQ"
    if not interval:
        interval = "1min"
    if not data_type:
        data_type = "CSV"
    querystring = {
        "currency": currency,
        "exchange": exchange,
        "symbol": index_symbol,
        "interval": interval,
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

    results = recover_get_response(
        querystring=querystring,
        request_type="time_series",
        data_type=data_type,
        api_type=api_type,
    )

    return results


# TODO - "verbose" edit - parameter controlling showing prints in console
def download_full_index_history(index_symbol: str, interval=None, exchange=None, currency=None):
    if not interval:
        interval = "1min"
    if not exchange:
        exchange = "NASDAQ"
    if not currency:
        currency = "USD"

    # decide which api to use first and start tracking api usage
    api_type = "rapid"
    rapid_used = 0
    regular_used = 0

    # we need to sleep 8 seconds after this - API provider uses averaging and too many
    # requests in short time will mean "no more tries for you pal"
    # In other words - even if you do "counting per minute",
    # depleting all the tokens in first few seconds of that minute will result in an error on another read
    # if proceeded too fast, even after clocked 60 seconds pass
    earliest_timestamp = obtain_earliest_timestamp(index_symbol, api_type=api_type)
    # rapid_count_per_minute += 1
    sleep(8)

    work_period_start = time()
    full_time_series = []
    print("target timestamp", earliest_timestamp['datetime'], work_period_start)
    first_historical_point = datetime.strptime(earliest_timestamp['datetime'], '%Y-%m-%d %H:%M:%S')
    download_params = {
        "index_symbol": index_symbol,
        "interval": interval,
        "exchange": exchange,
        "currency": currency,
        # date params make it inefficient in terms of obtainable 5k data points per query
        "end_date": None,
        # "end_date": time_period+timedelta(days=20),
        # "start_date": time_period,
        # "date": time_period,
        "data_type": "json",
    }

    for j in range(100):  # limited number of repeats just for safety - ~499k points potential anyway...
        partial_data = download_time_series(**download_params, api_type=api_type)

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
        if len(full_time_series) > 0:
            print("last record tracked:", full_time_series[-1])
            print(f"start of the current batch: {starting_record}")
            print("last record of current batch:", last_record)
        else:
            print("first batch")
            print(f"start of the current batch: {starting_record}")
            print("last record of current batch:", last_record)

        print("downloaded rows", len(d['values']))
        time_period = datetime.strptime(last_record['datetime'], '%Y-%m-%d %H:%M:%S')

        print("extending with removing duplicate row (new start: ", d['values'][1:][0], ")")
        full_time_series.extend(d['values'][1:])

        # check ending condition
        if time_period == first_historical_point:
            print('end of download reached')
            break

        print(f'next dump up until following date: {time_period}')

        # not finished yet? keep api usage in check to not get any errors in output
        if api_type == "rapid":
            rapid_used += 1
        elif api_type == "regular":
            regular_used += 1
        print("api usage count:", (rapid_used, regular_used))

        if rapid_used > 0:
            print("api type switch")
            api_type = "regular"

        if rapid_used > 0 and regular_used > 0:  # TODO - light edit - switch to true/false
            print("api tokens depleted, switching back to rapid and sleep")
            api_type = "rapid"
            sleep(7.4)
            rapid_used = 0
            regular_used = 0

        # params refresh after recent download
        download_params = {
            "index_symbol": index_symbol,
            "interval": interval,
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
    stock = "OTEX"
    # following is ~250k rows download
    time_series = download_full_index_history(index_symbol=stock)
    with open('../db_functions/otex_series.txt', 'w', encoding="UTF-8", newline='\n') as api_test:
        for entry in time_series:
            api_test.write(str(entry) + "\n")
