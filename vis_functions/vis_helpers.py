from datetime import datetime
from typing import Literal, Optional
# from

MINUTELY_CHART_SPLITTERS = [
    *[(5*i, 15*i, 40*i) for i in range(1, 15)], (120, 360, 720), (180, 1080, 3240)]
DAY_LABEL_DRAW_EXCLUSION = [
    (i, 10 + sum([15 + 4*j for j in range(i)])) for i in range(9)]  # what day will have a timestamp shown in chart
MONTH_LABEL_DRAW_EXCLUSION = [
    (i, 20 + sum([10 + 3*j for j in range(i)])) for i in range(9)]
AVAILABLE_THEMES = Optional[Literal[
    "OSCILLOSCOPE_FROM_90s", "FREEDOM24_DARK_TEAL", "TRADINGVIEW_WHITE", "NINJATRADER_SLATE_DARK"]]

print(MINUTELY_CHART_SPLITTERS)
print(DAY_LABEL_DRAW_EXCLUSION)
print(MONTH_LABEL_DRAW_EXCLUSION)


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
        "CHART_TEXT_FONT_SIZE": 9,
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(5, 177, 17),
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(4, 113, 12),
        "BULL_CANDLE_BODY": rgb_to_matlab(4, 222, 14),
        "BEAR_CANDLE_BODY": rgb_to_matlab(3, 9, 3),
        "CANDLE_BORDER": rgb_to_matlab(4, 222, 14),
        "VOLUME_BAR_BODY": rgb_to_matlab(4, 120, 14),
        "WICK_COLOR": rgb_to_matlab(4, 222, 14),
    },
    "FREEDOM24_DARK_TEAL": {
        "CHART_MAIN_PLOT": rgb_to_matlab(118, 121, 132),
        "CHART_TICKS_SPINES": rgb_to_matlab(75, 77, 82),
        "CHART_BG": rgb_to_matlab(30, 34, 54),  # added a bit blueish
        "CHART_TEXT_COLOR": rgb_to_matlab(138, 141, 145),
        "CHART_TEXT_FONT": 'Segoe UI',  # not original one, but from inspect it is one that they use
        "CHART_TEXT_FONT_SIZE": 8,
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(97, 98, 101),
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(42, 46, 57),
        "BULL_CANDLE_BODY": rgb_to_matlab(38, 166, 154),
        "BEAR_CANDLE_BODY": rgb_to_matlab(239, 83, 80),
        "CANDLE_BORDER": None,  # candle border is the same color as the candle
        "VOLUME_BAR_BODY": rgb_to_matlab(34, 55, 98),  # a bit modified
        "WICK_COLOR": rgb_to_matlab(118, 121, 132),
    },
    "TRADINGVIEW_WHITE": {
        "CHART_MAIN_PLOT": rgb_to_matlab(68, 138, 255),
        "CHART_TICKS_SPINES": rgb_to_matlab(255, 255, 255),
        "CHART_BG": rgb_to_matlab(255, 255, 255),
        "CHART_TEXT_COLOR": rgb_to_matlab(19, 23, 34),
        "CHART_TEXT_FONT": 'Trebuchet MS',
        "CHART_TEXT_FONT_SIZE": 10,
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(149, 152, 161),
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(243, 243, 243),
        "BULL_CANDLE_BODY": rgb_to_matlab(8, 153, 129),
        "BEAR_CANDLE_BODY": rgb_to_matlab(242, 54, 69),
        "CANDLE_BORDER": None,   # like above
        "VOLUME_BAR_BODY": 0.5,  # about 50% brighter -> +50% of the diff from 255 to main
        # volume candles are also transparent
        "WICK_COLOR": None,
    },
    "NINJATRADER_SLATE_DARK": {
        "CHART_MAIN_PLOT": rgb_to_matlab(255, 255, 255),
        "CHART_TICKS_SPINES": rgb_to_matlab(255, 255, 255),
        "CHART_BG": rgb_to_matlab(4, 4, 4),
        "CHART_TEXT_COLOR": rgb_to_matlab(255, 255, 255),
        "CHART_TEXT_FONT": 'Arial',
        "CHART_TEXT_FONT_SIZE": 7,
        "CHART_MAIN_SEPARATORS": rgb_to_matlab(120, 120, 120),  # there are no main separator in my setting
        "CHART_SMALL_SEPARATORS": rgb_to_matlab(80, 80, 80),
        "BULL_CANDLE_BODY": rgb_to_matlab(53, 206, 53),
        "BEAR_CANDLE_BODY": rgb_to_matlab(68, 107, 255),
        "CANDLE_BORDER": rgb_to_matlab(255, 255, 255),
        "VOLUME_BAR_BODY": rgb_to_matlab(34, 146, 255),
        "WICK_COLOR": rgb_to_matlab(255, 255, 255),
    },
}


def exclusion_zone_marker(exclusion_list: list[bool], penalty, major_index):
    print(f"excluding from {major_index-penalty} to {major_index + penalty}")
    for index in range(major_index-penalty, major_index+penalty+1):
        try:
            exclusion_list[index] = True
        except IndexError:
            pass
    return exclusion_list


def major_split_chart(
        data: list[datetime], timeframe: str, chart_graphical_aspect_ratio: float) -> list[tuple[bool, bool]]:
    """
    Denote where to put a heavier line on every new market daily open in case of minutely chart, or on every new month
    in the case of daily candles, on forex chart, just denote a beginning of a new day or month.

    This is actually equivalent in data timestamps for both, but for stocks day happens to be a new market
    open in the morning, while forex has 24h cycle of trading.

    the list of tuples that come out of this function marks following:
        [(place_big_divider, prevent_small_label_drawing)]
    for each of the timestamps that will form respective labels in the chart
    """

    # decide which param of the timestamp to use to evaluate chart timeline labeling, and select proper
    # penalty, against which smaller labels are prevented drawing, based on the distance away from main label
    # ("day" is main label in minute chart, "month" is main label in daily chart,
    # ("hours" - smaller timestamp in case of minute chart, while int("day") for daily chart, respectively)
    penalty = None
    if "day" in timeframe:
        param = "month"
        for p_tuple in MONTH_LABEL_DRAW_EXCLUSION:
            if len(data) < p_tuple[1]:
                penalty = p_tuple[0]
                break
        if not penalty:
            penalty = MONTH_LABEL_DRAW_EXCLUSION[-1][0]
    elif "h" in timeframe or "min" in timeframe:
        param = "day"
        for p_tuple in DAY_LABEL_DRAW_EXCLUSION:
            if len(data) < p_tuple[1]:
                penalty = p_tuple[0]
                break
        if not penalty:
            penalty = DAY_LABEL_DRAW_EXCLUSION[-1][0]
    else:
        # not found the timeframe, but return with no error -> no major labels
        return [(False, False) for _ in range(len(data))]

    # chart can get a bit dense when aspect ratio is not widescreen or 16:9, so we add a bit of
    # penalty to exclue farther
    if chart_graphical_aspect_ratio < 1.5: penalty += 1

    # too small data
    if len(data) == 1:
        return [(False, False)]

    # main loop of marking special chart labels
    marked_data = []
    small_label_exclusion_zone = [False for _ in range(len(data))]
    for i, timestamp in enumerate(data):

        if i == 0:
            if getattr(timestamp, param) != getattr(data[i+1], param):
                marked_data.append(False)
                marked_data.append(True)  # unfortunate case
                small_label_exclusion_zone = exclusion_zone_marker(small_label_exclusion_zone, penalty, i+1)
            else:
                marked_data.append(True)  # put a darker line prior to any candle/point on a chart
                marked_data.append(False)
                small_label_exclusion_zone = exclusion_zone_marker(small_label_exclusion_zone, penalty, i)
            continue
        if i == 1: continue

        try:
            if getattr(timestamp, param) != getattr(data[i-1], param):
                marked_data.append(True)
                small_label_exclusion_zone = exclusion_zone_marker(small_label_exclusion_zone, penalty, i)
            else:
                marked_data.append(False)
        except IndexError:
            marked_data.append(False)

    markers = [
        (major_label, minor_label_exclusion) for major_label, minor_label_exclusion
        in zip(marked_data, small_label_exclusion_zone)
    ]
    return markers


def chart_time_split(
        data: list[datetime], timeframe: str, chart_graphical_aspect_ratio: float
        ) -> list[tuple[bool, bool]]:
    """
    select periods of datapoints at which there should be visible label drawn on the chart

    this function also applies the day splitting in case of minute chart, or month splitting in the case of daily chart
    """
    chosen_splitter = None
    if "min" in timeframe:
        for splitter_definition in MINUTELY_CHART_SPLITTERS:
            if len(data) < splitter_definition[2]:
                chosen_splitter = splitter_definition[:-1]
                break

    if chosen_splitter is None:
        chosen_splitter = MINUTELY_CHART_SPLITTERS[-1][:-1]

    major_time_splits = major_split_chart(data, timeframe, chart_graphical_aspect_ratio)

    graphical_splitters: list[bool] = []
    if "min" in timeframe:
        for dp, major_split in zip(data, major_time_splits):
            market_open_ = datetime(year=dp.year, month=dp.month, day=dp.day, minute=30, hour=13)
            # in terms of real market data, following will get negative for forex, but it won't really matter i think
            minutes_from_open = abs((market_open_ - dp).total_seconds()) // 60
            # decide if there is need to put minor
            if (minutes_from_open % chosen_splitter[0] == 0) and (not major_split[1]):
                graphical_splitters.append(True)
            else:
                graphical_splitters.append(False)

    elif "day" in timeframe:
        for dp, major_split in zip(data, major_time_splits):
            # decide if there is need to put minor
            # split weeks on mondays
            if (dp.isoweekday() == 1) and (not major_split[1]):
                graphical_splitters.append(True)
            else:
                graphical_splitters.append(False)

    else:
        raise ValueError("timeframe not supported")

    label_list: list[tuple[bool, bool]] = [
        (major_labels[0], small_label)
        for major_labels, small_label
        in zip(major_time_splits, graphical_splitters)
    ]

    return label_list


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


def get_time_series_labels(
        time_splitting_spec: list[tuple[bool, bool]], timestamps: list[datetime], timeframe: str):
    """
    grabs a time splitting specification obtained from other helper function, and based on it generates
    series of labels to correctly
    """
    timestamp: datetime
    if "min" in timeframe:
        time_series_labels = []
        for (major_label, minor_label), (index, timestamp) in zip(time_splitting_spec, enumerate(timestamps)):
            if major_label:
                label = timestamp.strftime("%a")  # print day
            elif minor_label:
                label = timestamp.strftime("%H:%M")  # print hour:minute
            else:
                label = ""  # leave empty
            time_series_labels.append(label)

    elif "day" in timeframe:
        time_series_labels = []
        for (major_label, minor_label), (index, timestamp) in zip(time_splitting_spec, enumerate(timestamps)):
            if major_label:
                label = timestamp.strftime("%b")  # print short_month
            elif minor_label:
                label = f"{timestamp.day}"  # print int("day")
            else:
                label = ""  # leave empty
            time_series_labels.append(label)

    else:
        raise ValueError("timestamp not recognized")

    return time_series_labels


def define_bars_colors(open_series, close_series, color_factor=None, bull_bar_color=None, bear_bar_color=None):
    """
    create a series of colors that should be applied in a bar-by-bar fashion to the volume bar series
    the "color factor" applies a brightening effect (increases the saturation

    series of opens and closes will be used to determine if bar is bearish or bullish
    """
    if color_factor:
        if -1 >= color_factor >= 1:
            raise ValueError("color factor has to be ")
    colors = []
    # apply brightening effect by increase
    if bear_bar_color:
        if color_factor > 0:
            new_color = [c + (1 - c)*color_factor for c in bear_bar_color]
        else:
            new_color = [c * (1 + color_factor) for c in bear_bar_color]
        bear_bar_color = tuple(new_color)
    if bull_bar_color:
        if color_factor > 0:
            new_color = [c + (1 - c)*color_factor for c in bull_bar_color]
        else:
            new_color = [c * (1 + color_factor) for c in bull_bar_color]
        bull_bar_color = tuple(new_color)

    bull_bar_color = bull_bar_color if bull_bar_color else (0, 1, 0)
    bear_bar_color = bear_bar_color if bear_bar_color else (1, 0, 0)

    for open_, close_ in zip(open_series, close_series):
        if open_ < close_:
            colors.append(bull_bar_color)
        elif open_ > close_:
            colors.append(bear_bar_color)
        else:
            colors.append(None)

    return colors
