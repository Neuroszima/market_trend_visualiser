from datetime import datetime


MINUTELY_CHART_SPLITTERS = [(5, 15, 45), (15, 60, 180), (30, 180, 540), (60, 360, 1080), (180, 1080, 3240)]
DAILY_CHART_SPLITTERS = 1  # what day will have a timestamp shown in chart


def split_chart_daily(data: list[datetime], timeframe: str) -> list[bool]:
    """
    Denote where to put a heavier line on every new market daily open in case of minutely chart, or on every new month
    in the case of daily candles, on forex chart, just denote a beginning of a new day or month.

    This is actually equivalent in data timestamps for both, but for stocks day happens to be a new market
    open in the morning, while forex has 24h cycle of trading.
    """
    if len(data) == 1:
        return [False]
    marked_data = []
    for i, timestamp in enumerate(data):
        if i == 0:
            if timestamp.day != data[i+1].day:
                marked_data.append(False)
                marked_data.append(True)  # unfortunate case
            else:
                marked_data.append(True)  # put a darker line prior to any candle/point on a chart
                marked_data.append(False)
            continue
        if i == 1: continue
        if timestamp.day != data[i-1].day:
            marked_data.append(True)
        else:
            marked_data.append(False)
    return marked_data


def chart_split_lines(data: list[datetime], timeframe: str) -> list[tuple[bool, ...]]:
    """
    select periods of datapoints at which there should be visible divisor line drawn on the chart

    the second period is bigger, and denotes more "thick" line that should be drawn, for visual
    representation purposes and readability

    the "periodic line" will be drawn, if "minutes" from datetime will be exactly "modulo 0" over the chosen
    splitter period

    this function also applies the day splitting, or month splitting in the case of daily chart
    """
    chosen_splitter = None
    for splitter_definition in MINUTELY_CHART_SPLITTERS:
        if len(data) < splitter_definition[2]:
            chosen_splitter = splitter_definition[:-1]

    if chosen_splitter is None:
        chosen_splitter = MINUTELY_CHART_SPLITTERS[-1][:-1]

    major_time_splits = split_chart_daily(data, timeframe)

    graphical_splitters = []
    if "min" in timeframe:
        for dp in data:
            market_open_ = datetime(year=dp.year, month=dp.month, day=dp.day, minute=30, hour=13)
            # in terms of real market data, following will get negative for forex, but it won't really matter i think
            minutes_from_open = abs((market_open_ - dp).total_seconds()) // 60
            drawing_informations = (
                True if minutes_from_open % chosen_splitter[0] == 0 else False,  # very light and pale lines
                True if minutes_from_open % chosen_splitter[1] == 0 else False,  # slightly more visible lines
            )
            graphical_splitters.append((*dp, *drawing_informations))
    elif "day" in timeframe:
        for dp in data:
            drawing_info = (
                True if dp.isoweekday() == 1 else False,  # split weeks on mondays -> slightly more visible line
                False  # no thicker lines really
            )
            graphical_splitters.append((*dp, *drawing_info))
    else:
        raise ValueError("timeframe not supported")

    graphical_splitters = [
        (bigger_splitter, *small_splitters)
        for bigger_splitter, small_splitters
        in zip(major_time_splits, graphical_splitters)
    ]

    return graphical_splitters
