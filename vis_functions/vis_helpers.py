from datetime import datetime


MINUTELY_CHART_SPLITTERS = [(5, 15, 45), (15, 60, 180), (30, 180, 540), (60, 360, 1080), (180, 1080, 3240)]
DAILY_CHART_SPLITTERS = 1  # what day will have a timestamp shown in chart


def rgb_to_matlab(r, g, b) -> list | tuple:
    """
    :return: fraction list according to MATLAB format, that matplotlib accepts
    """
    return r/255., g/255., b/255.


CHART_SETTINGS = {
    "OSCILLOSCOPE_FROM_90s": {
        "CHART_MAIN_PLOT": rgb_to_matlab(4, 222, 14),
        "CHART_TICKS_SPINES": rgb_to_matlab(4, 147, 14),
        "CHART_BG": rgb_to_matlab(3, 9, 3),
        "CHART_TEXT_COLOR": rgb_to_matlab(4, 232, 14),
        "CHART_TEXT_FONT": 'monospace',
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(5, 177, 17),
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(4, 113, 12),
        "BULL_CANDLE_BODY": rgb_to_matlab(4, 222, 14),
        "BEAR_CANDLE_BODY": rgb_to_matlab(3, 9, 3),
        "CANDLE_BORDER": rgb_to_matlab(4, 222, 14),
        "VOLUME_BAR_BODY": rgb_to_matlab(4, 120, 14),
    },
    "DARK_TEAL_FREEDOM24": {
        "CHART_MAIN_PLOT": rgb_to_matlab(118, 121, 132),
        "CHART_TICKS_SPINES": rgb_to_matlab(75, 77, 82),
        "CHART_BG": rgb_to_matlab(30, 34, 45),
        "CHART_TEXT_COLOR": rgb_to_matlab(4, 232, 14),
        "CHART_TEXT_FONT": 'Segoe UI',  # not original one, but from inspect it is one that they use
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(97, 98, 101),
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(42, 46, 57),
        "BULL_CANDLE_BODY": rgb_to_matlab(38, 166, 154),
        "BEAR_CANDLE_BODY": rgb_to_matlab(239, 83, 80),
        "CANDLE_BORDER": None,  # candle border is the same color as the candle
        "VOLUME_BAR_BODY": rgb_to_matlab(30, 48, 90),  # a bit modified
    },
    "TRADINGVIEW_WHITE": {
        "CHART_MAIN_PLOT": rgb_to_matlab(68, 138, 255),
        "CHART_TICKS_SPINES": rgb_to_matlab(255, 255, 255),
        "CHART_BG": rgb_to_matlab(255, 255, 255),
        "CHART_TEXT_COLOR": rgb_to_matlab(19, 23, 34),
        "CHART_TEXT_FONT": 'Trebuchet MS',
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(149, 152, 161),
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(243, 243, 243),
        "BULL_CANDLE_BODY": rgb_to_matlab(8, 153, 129),
        "BEAR_CANDLE_BODY": rgb_to_matlab(242, 54, 69),
        "CANDLE_BORDER": None,   # like above
        "VOLUME_BAR_BODY": "+50%",  # about 50% brighter -> +50% of the diff from 255 to main
    # volume candles are also transparent
    },
    "NINJATRADER_SLATE_DARK": {
        "CHART_MAIN_PLOT": rgb_to_matlab(255, 255, 255),
        "CHART_TICKS_SPINES": rgb_to_matlab(255, 255, 255),
        "CHART_BG": rgb_to_matlab(4, 4, 4),
        "CHART_TEXT_COLOR": rgb_to_matlab(255, 255, 255),
        "CHART_TEXT_FONT": 'Arial',
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(120, 120, 120),  # there are no main separator in my setting
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(80, 80, 80),
        "BULL_CANDLE_BODY": rgb_to_matlab(53, 206, 53),
        "BEAR_CANDLE_BODY": rgb_to_matlab(68, 107, 255),
        "CANDLE_BORDER": rgb_to_matlab(255, 255, 255),
        "VOLUME_BAR_BODY": rgb_to_matlab(34, 146, 255),
    },
}


def split_chart_daily(data: list[datetime], timeframe: str) -> list[bool]:
    """
    Denote where to put a heavier line on every new market daily open in case of minutely chart, or on every new month
    in the case of daily candles, on forex chart, just denote a beginning of a new day or month.

    This is actually equivalent in data timestamps for both, but for stocks day happens to be a new market
    open in the morning, while forex has 24h cycle of trading.
    """
    if "day" in timeframe:
        param = "month"
    elif "h" in timeframe or "min" in timeframe:
        param = "day"
    else:
        return [False for _ in range(len(data))]
    if len(data) == 1:
        return [False]
    marked_data = []
    for i, timestamp in enumerate(data):
        if i == 0:
            if getattr(timestamp, param) != getattr(data[i+1], param):
                marked_data.append(False)
                marked_data.append(True)  # unfortunate case
            else:
                marked_data.append(True)  # put a darker line prior to any candle/point on a chart
                marked_data.append(False)
            continue
        if i == 1: continue
        if getattr(timestamp, param) != getattr(data[i+1], param):
            marked_data.append(True)
        else:
            marked_data.append(False)
    return marked_data


def chart_split_lines(data: list[datetime], timeframe: str) -> list[tuple[bool, bool, bool]]:
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

    graphical_splitters: list[tuple[bool, bool]] = []
    if "min" in timeframe:
        for dp in data:
            market_open_ = datetime(year=dp.year, month=dp.month, day=dp.day, minute=30, hour=13)
            # in terms of real market data, following will get negative for forex, but it won't really matter i think
            minutes_from_open = abs((market_open_ - dp).total_seconds()) // 60
            drawing_informations = (
                True if minutes_from_open % chosen_splitter[0] == 0 else False,  # very light and pale lines
                True if minutes_from_open % chosen_splitter[1] == 0 else False,  # slightly more visible lines
            )
            graphical_splitters.append(drawing_informations)
    elif "day" in timeframe:
        for dp in data:
            drawing_info = (
                True if dp.isoweekday() == 1 else False,  # split weeks on mondays -> slightly more visible line
                False  # no thicker lines really
            )
            graphical_splitters.append(drawing_info)
    else:
        raise ValueError("timeframe not supported")

    graphical_splitters: list[tuple[bool, bool, bool]] = [
        (bigger_splitter, small_splitters[0], small_splitters[1])
        for bigger_splitter, small_splitters
        in zip(major_time_splits, graphical_splitters)
    ]

    return graphical_splitters


def resolve_timeframe_name(timeframe: str):
    """get the full name of timeframe used to prepare the chart"""
    digits = "".join([c for c in timeframe if c.isdigit()])
    remainder = timeframe[len(digits):]
    if remainder == "min":
        full_name = "minute"
    elif remainder == "h":
        full_name = "hour"
    elif remainder in ["day", "week", "month"]:
        full_name = remainder
    else:
        raise ValueError("timeframe not suitable for this application")
    return digits + " " + full_name
