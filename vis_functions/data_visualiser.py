from datetime import datetime
from functools import partial
from itertools import product
from math import floor
from os.path import abspath
from typing import Literal, Callable

from matplotlib import pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter  # noqa
from matplotlib.axes import Axes
from matplotlib.axis import Axis
from matplotlib.patches import Rectangle
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.spines import Spine
from matplotlib.ticker import Formatter

# following is a helper import, only for typehinting/autocompletion
try:
    from charts.base_axes_pyi_definitions import AxesBase
except ImportError:
    AxesBase = None

import vis_functions.vis_helpers as vis_helpers
from vis_functions.vis_helpers import AVAILABLE_THEMES_, CHART_SETTINGS_

CHART_FILE_LOCATION_ = "\\".join(abspath(__file__).split("\\")[:-2] + ["charts"])
_SERIES_DEFINER = Literal['close', 'open', 'volume', 'low', 'high']


class PriceChart:
    """
    graphically present financial data from provider

    data input can be a directly downloaded data, or data coming from database fetch request
    """

    def __init__(self, data: list[tuple] | list[dict], symbol: str, timeframe: str, market_code: str,
                 is_equity_: bool, size_x: int, size_y: int, dpi: int = 300, color_theme: AVAILABLE_THEMES_ = None,
                 chart_type: Literal['simple_open', 'simple_close', 'candlestick'] = None):
        # disassemble data for various purposes
        # for example we could "show close" only, or draw entire candlesticks
        self.timeframe = timeframe
        self.market_code = market_code
        self.is_equity = is_equity_
        self.symbol = symbol
        self._series_timestamps: list[datetime] | None = None
        self._series_lows: list[float | int] | None = None
        self._series_highs: list[float | int] | None = None
        self._series_opens: list[float | int] | None = None
        self._series_closes: list[float | int] | None = None
        self._series_volumes: list[int] | None = None
        self._disassemble(data, is_equity_)

        # chart settings
        self.size_x = size_x
        self.size_y = size_y
        self.main_chart_bottom: int | float | None = None
        self.main_chart_top: int | float | None = None
        self.chart_splits = vis_helpers.chart_time_split_(
            self._series_timestamps, self.timeframe, self.size_x / self.size_y)
        self.title: str | None = None
        self.figure: Figure | None = None
        self.main_chart: Axes | AxesBase = None
        self.volume_chart: Axes | AxesBase = None
        self.dpi = dpi
        self.chart_timeline_bound = int(len(self._series_highs)*1.032)+1
        self.color_theme = "DARK_TEAL_FREEDOM24" if color_theme is None else color_theme
        self.chart_type = "simple_close" if chart_type is None else chart_type
        self.contour_width = max(0.26, (62 / len(data)) ** (1. / 3) * 0.43)
        self.candle_width = 0.55
        self.main_font_size = CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT_SIZE"] * 250 / self.dpi
        self.labelsize: int | float | None = None
        self.date_labelsize: int | float | None = None
        self.main_chart_title_offset = 0.002 * self.main_font_size

    def _disassemble(self, data: list[tuple | dict], is_stock: bool):
        """disassemble time series data into separate components for future reference"""
        # get to know if elements are dicts or tuples (provider origin or db origin)
        if isinstance(data[0], tuple):
            # data from database
            self._series_timestamps = [d[1] for d in data]  # already a datetime object
            self._series_opens = [d[2] for d in data]
            self._series_closes = [d[3] for d in data]
            self._series_highs = [d[4] for d in data]
            self._series_lows = [d[5] for d in data]
            if is_stock:
                self._series_volumes = [d[6] for d in data]
        elif isinstance(data[0], dict):
            # data downloaded
            # convert timestamps into datetime objects
            if self.timeframe in ['1day']:
                time_conversion = '%Y-%m-%d'
            elif self.timeframe in ['1min']:
                time_conversion = '%Y-%m-%d %H:%M:%S'
            else:
                raise ValueError(self.timeframe)
            self._series_timestamps = [datetime.strptime(d["datetime"], time_conversion) for d in data]
            self._series_opens = [d["open"] for d in data]
            self._series_closes = [d["close"] for d in data]
            self._series_highs = [d["high"] for d in data]
            self._series_lows = [d["low"] for d in data]
            if is_stock:
                self._series_volumes = [d["volume"] for d in data]
        else:
            raise TypeError('Unsupported data type for disassemble')

    def _choose_right_labelsize(self):
        """this has to be called only after disassemble"""
        # usually forex pairs come with "0. ..." and about 5 digits after comma, thus "7"
        if not self.is_equity:
            max_number_len = len(str(floor(max(self._series_highs))))
            # we acknowledge, that there could be a pair like "XAUUSD" where there is 4 digits prior to comma
            # then we "let up to 6th total digits" to appear in addition to ones already appearing in price
            max_number_len = max_number_len + max(2, 7 - max_number_len)
        else:
            max_number_len = len(str(floor(max(self._series_highs)))) + 3  # common price + ".XX" digits as "cents"

        if CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT"] == "monospace":
            label_factor = 1.05
        else:
            label_factor = 0.75
        l_size_start = (CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT_SIZE"] - label_factor) * 250 / self.dpi
        number_labels = min(l_size_start * (4 / max(4, max_number_len)) * 1.3, l_size_start)
        self.date_labelsize = l_size_start
        self.labelsize = number_labels

    def prepare_chart_space(self) -> Figure:
        if self._series_volumes:
            subfigure_height_ratios = [0.7, 0.3]  # main_chart ratio, volume chart ratio
        else:
            subfigure_height_ratios = [1, 0]  # no volume chart, subplot will have axis hidden

        figure: Figure = Figure(
            figsize=(self.size_x / self.dpi, self.size_y / self.dpi),
            facecolor=CHART_SETTINGS_[self.color_theme]['CHART_BG'],
        )
        figure.subplots(
            nrows=2, ncols=1, subplot_kw={"facecolor": CHART_SETTINGS_[self.color_theme]['CHART_BG']}, sharex=True,
            gridspec_kw={
                "height_ratios": subfigure_height_ratios, "hspace": 0,
                "top": 0.99, "bottom": 0.04, "left": 0.02, "right": 0.925
            }
        )
        self.main_chart: Axes | AxesBase = figure.get_axes()[0]
        self.volume_chart: Axes | AxesBase = figure.get_axes()[1]
        timeframe = vis_helpers.resolve_timeframe_name_(self.timeframe)
        self.title = f"{self.symbol}_{self.market_code}" if self.is_equity else f"{self.symbol}"
        self.title += ", " + f"{timeframe}"
        for chart in [self.main_chart, self.volume_chart]:
            chart.spines: dict[str, Spine]  # noqa
            for e, spine in chart.spines.items():
                spine.set_linewidth(0.5)
                if e in ["left", 'top']:
                    spine.set_visible(False)
                if e == 'bottom':
                    spine.set_bounds(0, len(self._series_timestamps))
                spine.set(color=CHART_SETTINGS_[self.color_theme]["CHART_SMALL_SEPARATORS"])

        if len(self._series_timestamps) > 100:
            time_ticks_visible = False
        else:
            time_ticks_visible = True

        self.main_chart.set_title(
            self.title, loc="left", fontstyle='normal', fontweight='normal',
            fontvariant='small-caps', y=0.93-self.main_chart_title_offset,
            color=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_COLOR"],
            fontfamily=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT"],
            fontsize=self.main_font_size, fontstretch='ultra-condensed'
        )

        for chart in [self.main_chart, self.volume_chart]:
            chart.yaxis: Axis  # noqa
            chart.yaxis.set_tick_params(
                which="major", left=False, right=True, direction="in",
                color=CHART_SETTINGS_[self.color_theme]["CHART_MAIN_SEPARATORS"],
                labelcolor=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_COLOR"],
                labelsize=self.labelsize,
                labelfontfamily=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT"],
                labelleft=False, labelright=True,
            )

            chart.xaxis: Axis  # noqa
            chart.xaxis.set_tick_params(
                which="major", direction="in", bottom=time_ticks_visible,
                color=CHART_SETTINGS_[self.color_theme]["CHART_MAIN_SEPARATORS"],
                labelcolor=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_COLOR"],
                labelsize=self.date_labelsize,
                labelfontfamily=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT"],
            )

        for ax in figure.get_axes():  # make sure only bottom labels are applied
            ax.label_outer()

        if not self._series_volumes:
            self.volume_chart.yaxis.set_visible(False)
        else:
            for ax in figure.get_axes():  # make sure only bottom labels are applied
                ax.xaxis.set_tick_params(bottom=False)

        return figure

    def prescale_main_chart(self):
        """apply a proper scaling to the chart so all "candle" actors start to become visible"""
        flat_offset = float(max(self._series_highs) - min(self._series_lows)) * 0.1
        self.main_chart_bottom = float(min(self._series_lows)) - 0.8*flat_offset
        self.main_chart_top = float(max(self._series_highs)) + 1.6*flat_offset

        self.main_chart.set_ylim(bottom=self.main_chart_bottom, top=self.main_chart_top)
        self.main_chart.set_xlim(left=-1, right=self.chart_timeline_bound)

    def draw_simple_chart(self, X: list[int], which_series: _SERIES_DEFINER | None = None):
        """draw a single line showing price changes over time, usually in "candle close" format"""
        self.prescale_main_chart()
        if which_series:
            Y = getattr(self, f"_series_{which_series}s")
        else:
            Y = getattr(self, f"_series_{self.chart_type.split('_')[1]}s")
        self.main_chart.plot(
            X, Y, linewidth=0.7, color=CHART_SETTINGS_[self.color_theme]['CHART_MAIN_PLOT']
        )

    def draw_candlestick_chart(self):
        """after prescaling, add all the candle bodies with wicks to the main chart space"""
        self.prescale_main_chart()
        for i, (open_, close_, low_, high_) in enumerate(zip(
                self._series_opens, self._series_closes, self._series_lows, self._series_highs)):

            candle_height = close_ - open_  # DIRECTION MATTERS! no abs()
            doji_candle_height = 0.003 * float(max(self._series_highs) - min(self._series_lows))

            # determine wick color and candle color -> if "wick = None" -> inherit candle color
            if candle_height > 0:
                facecolor = CHART_SETTINGS_[self.color_theme]["BULL_CANDLE_BODY"]
            elif candle_height < 0:
                facecolor = CHART_SETTINGS_[self.color_theme]["BEAR_CANDLE_BODY"]
            else:
                facecolor = CHART_SETTINGS_[self.color_theme]["CHART_MAIN_PLOT"]
            wick_color = CHART_SETTINGS_[self.color_theme]["WICK_COLOR"] if \
                CHART_SETTINGS_[self.color_theme]["WICK_COLOR"] is not None else facecolor

            if open_ != close_:
                # added this for very slim candles, so that they can be visible
                if abs(candle_height) > abs(doji_candle_height):
                    height = candle_height
                else:
                    height = doji_candle_height
                candle_body = Rectangle(
                    (i - self.candle_width / 2, float(open_)), self.candle_width, height=float(height),
                    facecolor=facecolor, edgecolor=CHART_SETTINGS_[self.color_theme]["CANDLE_BORDER"],
                    linewidth=self.contour_width, zorder=5
                )
            else:  # doji candle
                # set the "semi-static" height based on the span of the data, so that it will scale even
                # when we change resolutions and data
                candle_body = Rectangle(
                    (i - self.candle_width / 2, float(open_) - doji_candle_height / 2), self.candle_width,
                    doji_candle_height,
                    color=facecolor, linewidth=self.contour_width, zorder=5
                )

            candle_wicks = Line2D(
                xdata=[i, i], ydata=[low_, high_], linewidth=self.contour_width, zorder=0, color=wick_color
            )
            self.main_chart.add_artist(candle_wicks)
            self.main_chart.add_artist(candle_body)

    def draw_volume_chart(self, X: list[int]):
        """scale and draw volume bars at the bottom of main plot"""
        self.volume_chart.set_title(
            f"VOL ({self.timeframe})", loc="left", fontstyle='normal', fontweight='normal',
            y=0.81 - self.main_chart_title_offset, fontsize=self.main_font_size, fontstretch='ultra-condensed',
            fontvariant='small-caps', color=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_COLOR"],
            fontfamily=CHART_SETTINGS_[self.color_theme]["CHART_TEXT_FONT"],
        )
        volume_bar_color = CHART_SETTINGS_[self.color_theme]["VOLUME_BAR_BODY"]

        # custom volume chart bar coloring
        if isinstance(volume_bar_color, float):  # this means we should derive it from regular candle body
            bar_colors = vis_helpers.define_bars_colors_(
                self._series_opens, self._series_closes, color_factor=volume_bar_color,
                bull_bar_color=CHART_SETTINGS_[self.color_theme]["BULL_CANDLE_BODY"],
                bear_bar_color=CHART_SETTINGS_[self.color_theme]["BEAR_CANDLE_BODY"],
            )
            # fixing "None"s and replacing them with default color that should be shown in the volume chart
            bar_colors = [c if c is not None else CHART_SETTINGS_[self.color_theme]["CHART_MAIN_PLOT"]
                          for c in bar_colors]
        elif isinstance(volume_bar_color, tuple):
            bar_colors = volume_bar_color
        else:
            bar_colors = CHART_SETTINGS_[self.color_theme]["CHART_MAIN_PLOT"]

        self.volume_chart.bar(
            x=X, height=self._series_volumes, width=self.candle_width * 0.9, align="center",
            color=bar_colors, linewidth=self.contour_width * 0.5
        )
        self.volume_chart.set_ylim(bottom=0, top=max(self._series_volumes) * 1.35)
        volume_label_formatter: Formatter | partial | Callable = partial(
            vis_helpers.volume_labels_ticker_, max_arg=max(self._series_volumes))
        self.volume_chart.yaxis.set_major_formatter(volume_label_formatter)

    def draw_end_price_label(self):
        connector, last_price_label = vis_helpers.draw_end_price_label_(
            self._series_closes[-1], self._series_opens[-1], len(self._series_timestamps)-1,
            self.chart_timeline_bound, label_size=self.labelsize, color_theme=self.color_theme,
            is_equity=self.is_equity,
        )
        self.main_chart.add_artist(connector)
        self.main_chart.add_artist(last_price_label)

    def save(self, name: str | None = None):
        if self.figure:
            if name:
                self.figure.savefig(CHART_FILE_LOCATION_ + "\\" + name + ".png", dpi=self.dpi)
            else:
                self.figure.savefig(CHART_FILE_LOCATION_ + "\\" + self.symbol, dpi=self.dpi)
            plt.close(self.figure)

    def make_chart(self, mode: str | None = None, chart_name: str | None = None):
        """complete procedure to make a chart space along with """
        self._choose_right_labelsize()
        line_splitting_spec = vis_helpers.chart_time_split_(
            self._series_timestamps, self.timeframe, self.size_x / self.size_y)
        X_labels = vis_helpers.get_time_series_labels(
            line_splitting_spec, self._series_timestamps, self.timeframe)
        X = [*range(len(self._series_timestamps))]
        self.figure = self.prepare_chart_space()
        if mode == "simple":
            self.draw_simple_chart(X)
        else:
            if "simple" in self.chart_type:
                self.draw_simple_chart(X)
            else:
                self.draw_candlestick_chart()
        if self._series_volumes:
            self.draw_volume_chart(X)
        self.draw_end_price_label()
        self.volume_chart.set_xticks(X, labels=X_labels)
        self.save(name=chart_name)


if __name__ == '__main__':
    from random import seed
    from tests.t_helpers import generate_random_time_sample

    seed(2346346)
    # very good seed accidentally -> test if 2 labels will overlap
    # badly ("Fri" label with adjacent "19:00") -> for 80 daily and 400 1min
    # bad formatting of 2018 date and squashed days -> 1day, 250 span. (couldn't fix properly)
    aspect_1 = (1920, 1080)
    aspect_2 = (1200, 1200)

    symbol, timeframe_, market_code, is_equity = "AAPL", "1min", "XNGS", True
    series_1min = generate_random_time_sample("1min", is_equity=is_equity, span=46)  # 46, 97, 250, 400
    series_1day = generate_random_time_sample("1day", is_equity=is_equity, span=46)  # 46, 97, 250, 68
    timeframes = ['1min', '1day']
    color_themes = [
        "NINJATRADER_SLATE_DARK", "FREEDOM24_DARK_TEAL",
        "OSCILLOSCOPE_FROM_90s", "TRADINGVIEW_WHITE"
    ]
    chart_types = ['simple_close', 'candlestick']
    for c_theme, tf, ch_type in product(color_themes, timeframes, chart_types):
        if tf == "1min":
            time_series = series_1min
        else:
            time_series = series_1day
        c = PriceChart(time_series, symbol, tf, market_code, is_equity, *aspect_2, dpi=300,
                       chart_type=ch_type, color_theme=c_theme)
        short_cs_name = "_".join(c_theme.split("_")[1:])
        c.make_chart(chart_name=f"{symbol}_{tf}_{short_cs_name}_{ch_type}")
