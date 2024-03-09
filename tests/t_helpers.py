from datetime import datetime, timedelta
from random import random, randint

from psycopg2 import connect

import db_functions.db_helpers as helpers


def time_bracket_case_generator(
        reference_dataset: list[dict], starting_timestamp: datetime,
        ending_timestamp: datetime, raised_exception: type | None = None):
    """
    this provides a way of constructing test subcases for testing particular function, as there is a
    lot of small, nuanced details that has to be checked

    if a subcase should raise exception, pass one as argument here
    """
    # get "id"s (here indexes) of points closest to starting/ending timestamps
    starting_index, ending_index = None, None
    for index, d in enumerate(reference_dataset):
        if (starting_index is None) and (d["datetime_object"] >= starting_timestamp):
            starting_index = index
        # the day prior to day that is "later" than ending timestamp
        # or -> the last day that suffice the "less than ending"
        if (ending_index is None) and (not d["datetime_object"] <= ending_timestamp):
            ending_index = index - 1

    if starting_index and (ending_index is None):
        if reference_dataset[-1]["datetime_object"] <= ending_timestamp:
            ending_index = len(reference_dataset) - 1

    # there are two edge cases:
    # 1. ending less than the starting of dataset
    # 2. starting greater than the ending of dataset
    # for these we ignore what this exact function outputs, because we should raise exception in main function anyway

    delta = ending_timestamp - starting_timestamp  # equivalent to 'time_span'
    span = ending_index - starting_index + 1  # equivalent to 'trading_time_span'
    answer = (starting_index, ending_index)
    cases = [
        # cases in format:
        #    start_date,         end_date,    time_span, trading_time_span,  predicted_answer,  raised_exception
        (starting_timestamp, ending_timestamp,   None,         None,              answer,       raised_exception),
        (starting_timestamp,      None,         delta,         None,              answer,       raised_exception),
        (       None,        ending_timestamp,  delta,         None,              answer,       raised_exception),
        (starting_timestamp,      None,          None,         span,              answer,       raised_exception),
        (       None,        ending_timestamp,   None,         span,              answer,       raised_exception)
    ]
    return cases


def generate_random_time_sample(
        time_interval: str, is_equity: bool, span: int = 10, monday_start=False) -> list[dict]:
    """
    generate a couple of data points for given time series

    reworked version skips Saturdays and Sundays, as well as allows for a bit more datapoints to be generated,
    for wider time period checks
    """
    if "day" in time_interval:
        start_day = datetime(year=randint(2010, 2022), month=randint(3, 12), day=randint(1, 30))
        delta = timedelta(days=1)
    elif "min" in time_interval:
        start_day = datetime(
            year=randint(2010, 2022), month=randint(3, 12), day=randint(1, 30),
            hour=randint(15, 20), minute=randint(30, 59)
        )
        delta = timedelta(minutes=1)
    elif "h" in time_interval:
        start_day = datetime(
            year=randint(2010, 2022), month=randint(3, 12), day=randint(1, 30), hour=randint(15, 20))
        delta = timedelta(hours=1)
    else:
        raise ValueError('time interval!')

    if monday_start:
        if (monday_check := start_day.isoweekday()) != 1:
            start_day = start_day - timedelta(days=monday_check - 1)
    else:
        if (weekday_check := start_day.isoweekday()) not in (1, 2, 3, 4, 5):
            start_day = start_day - timedelta(days=weekday_check - 5)

    dates = [start_day]
    old_date = start_day
    for i in range(span):
        new_date = old_date + delta
        if new_date.isoweekday() not in (1, 2, 3, 4, 5):
            new_date += timedelta(days=2)
        dates.append(new_date)
        old_date = new_date
    for d in dates:
        assert d.isoweekday() in (1, 2, 3, 4, 5), f"{d}, {time_interval}"

    candles = []
    for i in range(span):
        bullish = 0.5 > random()
        # following order will be low, open, close, high; bullish default
        candle = sorted([randint(4, 26) for _ in range(4)])
        if not bullish:
            candle[1], candle[2] = candle[2], candle[1]  # swap open with close
        if is_equity:
            candle.append(randint(100, 300))
        candles.append(candle)
    data = [
        {
            "datetime": str(date)[:-9] if "min" not in time_interval else str(date),
            "low": candle_[0], "open": candle_[1],
            "close": candle_[2], "high": candle_[3],
            # following is ignored by database insert functions, but is helpful for other tests
            "datetime_object": date,
        } for date, candle_ in zip(dates, candles)
    ]
    if is_equity:
        for point, candle in zip(data, candles):
            point['volume'] = candle[-1]
    return data


def pop_row_from_database(day_to_pop_index, schema_name, table_name):
    """remove single row from time series or another table, based on the ID (p-key)"""
    with connect(**helpers._connection_dict) as conn:
        cur = conn.cursor()
        cur.execute(helpers._delete_single_based_on_ID.format(
            schema_name=schema_name,
            table_name=table_name,
            index=day_to_pop_index,
        ))


if __name__ == '__main__':
    sample = generate_random_time_sample('1day', True, 45)
    print([(day['datetime'], day['datetime_object'].isoweekday()) for day in sample])
    print(next((day for day in sample if day['datetime_object'].isoweekday() == 5)))
