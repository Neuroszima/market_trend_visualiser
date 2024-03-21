from datetime import datetime
from math import floor
from os.path import abspath
from typing import Literal

from matplotlib import pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter  # noqa
from matplotlib.axes import Axes
from matplotlib.axis import Axis
from matplotlib.patches import Rectangle
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.spines import Spine

# following is a helper import, only for typehinting/autocompletion
try:
    from charts.base_axes_pyi_definitions import AxesBase
except ImportError:
    AxesBase = None

import vis_functions.vis_helpers as vis_helpers
from vis_functions.vis_helpers import AVAILABLE_THEMES, CHART_SETTINGS

CHART_FILE_LOCATION = "\\".join(abspath(__file__).split("\\")[:-2] + ["charts"])
SERIES_DEFINER = Literal['close', 'open', 'volume', 'low', 'high']


class PriceChart:
    """
    graphically present financial data from provider

    data input can be a directly downloaded data, or data coming from database fetch request
    """

    def __init__(self, data: list[tuple] | list[dict], symbol: str, timeframe: str, market_code: str,
                 is_stock_: bool, size_x: int, size_y: int, dpi: int = 300, color_theme: AVAILABLE_THEMES = None,
                 chart_type: Literal['simple_open', 'simple_close', 'candlestick'] = None):
        # disassemble data for various purposes
        # for example we could "show close" only, or draw entire candlesticks
        self.timeframe = timeframe
        self.market_code = market_code
        self.is_stock = is_stock_
        self.symbol = symbol
        self._series_timestamps: list[datetime] | None = None
        self._series_lows: list[float | int] | None = None
        self._series_highs: list[float | int] | None = None
        self._series_opens: list[float | int] | None = None
        self._series_closes: list[float | int] | None = None
        self._series_volumes: list[int] | None = None
        self._disassemble(data, is_stock_)

        # chart settings
        self.size_x = size_x
        self.size_y = size_y
        self.chart_splits = vis_helpers.chart_time_split(
            self._series_timestamps, self.timeframe, self.size_x / self.size_y)
        self.title: str | None = None
        self.figure: Figure | None = None
        self.main_chart: Axes | AxesBase = None
        self.volume_chart: Axes | AxesBase = None
        self.dpi = dpi
        self.color_theme = "DARK_TEAL_FREEDOM24" if color_theme is None else color_theme
        self.chart_type = "simple_close" if chart_type is None else chart_type
        self.contour_width = max(0.45, (62 / len(data)) ** (1. / 3) * 0.70)
        self.candle_width = 0.55
        self.main_font_size = CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT_SIZE"] * 250 / self.dpi
        self.labelsize = (CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT_SIZE"] - 0.75) * 250 / self.dpi
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
        if not self.is_stock:
            max_number_len = len(str(floor(max(self._series_highs))))
            # we acknowledge, that there could be a pair like "XAUUSD" where there is 4 digits prior to comma
            # then we "let up to 6th total digits" to appear in addition to ones already appearing in price
            max_number_len = max_number_len + max(2, 7 - max_number_len)
        else:
            max_number_len = len(str(floor(max(self._series_highs)))) + 2  # common price + 2 digits as "cents"
        right_labels_free_space = self.size_x * 0.06 / 4

    def prepare_chart_space(self) -> Figure:
        if self._series_volumes:
            subfigure_height_ratios = [0.7, 0.3]  # main_chart ratio, volume chart ratio
        else:
            subfigure_height_ratios = [1, 0]  # no volume chart, subplot will have axis hidden

        figure: Figure = plt.figure(
            # constrained_layout=True,
            figsize=(self.size_x / self.dpi, self.size_y / self.dpi),
            facecolor=CHART_SETTINGS[self.color_theme]['CHART_BG'],
            # facecolor=(1, 1, 1),
        )
        figure.subplots(
            nrows=2, ncols=1, subplot_kw={"facecolor": CHART_SETTINGS[self.color_theme]['CHART_BG']}, sharex=True,
            gridspec_kw={
                "height_ratios": subfigure_height_ratios, "hspace": 0,
                "top": 0.99, "bottom": 0.04, "left": 0.02, "right": 0.94
            }
        )
        self.main_chart: Axes | AxesBase = figure.get_axes()[0]
        self.volume_chart: Axes | AxesBase = figure.get_axes()[1]
        timeframe = vis_helpers.resolve_timeframe_name(self.timeframe)
        self.title = f"{self.symbol}_{self.market_code}" if self.is_stock else f"{self.symbol}"
        self.title += ", " + f"{timeframe}"
        for chart in [self.main_chart, self.volume_chart]:
            chart.spines: dict[str, Spine]  # noqa
            for e, spine in chart.spines.items():
                spine.set_linewidth(0.5)
                if e in ["left", 'top']:
                    spine.set_visible(False)
                spine.set(color=CHART_SETTINGS[self.color_theme]["CHART_SMALL_SEPARATORS"])

        if len(self._series_timestamps) > 100:
            time_ticks_visible = False
        else:
            time_ticks_visible = True

        self.main_chart.set_title(
            self.title, loc="left", fontstyle='normal', fontweight='normal',
            fontvariant='small-caps', y=0.93-self.main_chart_title_offset,
            color=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
            fontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
            fontsize=self.main_font_size, fontstretch='ultra-condensed'
        )

        for chart in [self.main_chart, self.volume_chart]:
            chart.yaxis: Axis  # noqa
            chart.yaxis.set_tick_params(
                which="major", left=False, right=True, direction="in",
                color=CHART_SETTINGS[self.color_theme]["CHART_MAIN_SEPARATORS"],
                labelcolor=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
                labelsize=self.labelsize,
                labelfontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
                labelleft=False, labelright=True,
            )

            chart.xaxis: Axis  # noqa
            chart.xaxis.set_tick_params(
                which="major", direction="in", bottom=time_ticks_visible,
                color=CHART_SETTINGS[self.color_theme]["CHART_MAIN_SEPARATORS"],
                labelcolor=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
                labelsize=self.labelsize,
                labelfontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
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
        flat_offset = (max(self._series_highs) - min(self._series_lows)) * 0.1
        self.main_chart.set_ylim(
            bottom=min(self._series_lows) - 0.8*flat_offset, top=max(self._series_highs) + 1.6*flat_offset
        )
        self.main_chart.set_xlim(left=-1, right=len(self._series_highs))

    def draw_simple_chart(self, X: list[int], which_series: SERIES_DEFINER | None = None):
        """draw a single line showing price changes over time, usually in "candle close" format"""
        self.prescale_main_chart()
        if which_series:
            Y = getattr(self, f"_series_{which_series}s")
        else:
            Y = getattr(self, f"_series_{self.chart_type.split('_')[1]}s")
        self.main_chart.plot(
            X, Y, linewidth=0.7,
            color=CHART_SETTINGS[self.color_theme]['CHART_MAIN_PLOT']
        )

    def draw_candlestick_chart(self):
        """after prescaling, add all the candle bodies with wicks to the main chart space"""
        self.prescale_main_chart()
        for i, (open_, close_, low_, high_) in enumerate(zip(
                self._series_opens, self._series_closes, self._series_lows, self._series_highs)):

            candle_height = close_ - open_  # DIRECTION MATTERS! no abs()

            # determine wick color and candle color -> if "wick = None" -> inherit candle color
            if candle_height > 0:
                facecolor = CHART_SETTINGS[self.color_theme]["BULL_CANDLE_BODY"]
            elif candle_height < 0:
                facecolor = CHART_SETTINGS[self.color_theme]["BEAR_CANDLE_BODY"]
            else:
                facecolor = CHART_SETTINGS[self.color_theme]["CHART_MAIN_PLOT"]
            wick_color = CHART_SETTINGS[self.color_theme]["WICK_COLOR"] if \
                CHART_SETTINGS[self.color_theme]["WICK_COLOR"] is not None else facecolor

            if open_ != close_:
                candle_body = Rectangle(
                    (i - self.candle_width / 2, open_), self.candle_width, candle_height,
                    facecolor=facecolor, edgecolor=CHART_SETTINGS[self.color_theme]["CANDLE_BORDER"],
                    linewidth=self.contour_width, zorder=5
                )
            else:  # doji candle
                # set the "semi-static" height based on the span of the data, so that it will scale even
                # when we change resolutions and data
                candle_height = 0.005 * (max(self._series_highs) - min(self._series_lows))
                candle_body = Rectangle(
                    (i - self.candle_width / 2, open_ - candle_height / 2), self.candle_width, candle_height,
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
            fontvariant='small-caps', color=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
            fontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
        )
        volume_bar_color = CHART_SETTINGS[self.color_theme]["VOLUME_BAR_BODY"]

        # custom volume chart bar coloring
        if isinstance(volume_bar_color, float):  # this means we should derive it from regular candle body
            bar_colors = vis_helpers.define_bars_colors(
                self._series_opens, self._series_closes, color_factor=volume_bar_color,
                bull_bar_color=CHART_SETTINGS[self.color_theme]["BULL_CANDLE_BODY"],
                bear_bar_color=CHART_SETTINGS[self.color_theme]["BEAR_CANDLE_BODY"],
            )
            # fixing "None"s and replacing them with default color that should be shown in the volume chart
            bar_colors = [c if c is not None else CHART_SETTINGS[self.color_theme]["CHART_MAIN_PLOT"]
                          for c in bar_colors]
        elif isinstance(volume_bar_color, tuple):
            bar_colors = volume_bar_color
        else:
            bar_colors = CHART_SETTINGS[self.color_theme]["CHART_MAIN_PLOT"]

        self.volume_chart.bar(
            x=X, height=self._series_volumes, width=self.candle_width * 0.9, align="center",
            color=bar_colors, linewidth=self.contour_width * 0.5
        )
        self.volume_chart.set_ylim(bottom=0, top=max(self._series_volumes) * 1.35)

    def save(self, name: str | None = None):
        if self.figure:
            if name:
                self.figure.savefig(CHART_FILE_LOCATION + "\\" + name + ".png", dpi=self.dpi)
            else:
                self.figure.savefig(CHART_FILE_LOCATION + "\\" + self.symbol, dpi=self.dpi)

    def make_chart(self, mode: str | None = None, chart_name: str | None = None):
        """complete procedure to make a chart space along with """
        line_splitting_spec = vis_helpers.chart_time_split(
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
        self.volume_chart.set_xticks(X, labels=X_labels)
        self.save(name=chart_name)


if __name__ == '__main__':
    from random import seed
    from tests.t_helpers import generate_random_time_sample

    seed(2346346)
    # very good seed accidentally -> test if 2 labels will overlap
    # badly ("Fri" label with adjacent "19:00") -> for 80 daily and 400 1min
    aspect_1 = (1920, 1080)
    aspect_2 = (1200, 1200)

    symbol, timeframe_, market_code, is_equity = "AAPL", "1min", "XNGS", True
    series_1min = generate_random_time_sample("1min", is_equity=is_equity, span=250)  # 400
    series_1day = generate_random_time_sample("1day", is_equity=is_equity, span=250)  # 68
    c = PriceChart(series_1min, symbol, "1min", market_code, is_equity, *aspect_2, dpi=300, chart_type='simple_close',
                   color_theme="NINJATRADER_SLATE_DARK")
                   # color_theme="FREEDOM24_DARK_TEAL")
                   # color_theme="OSCILLOSCOPE_FROM_90s")
                   # color_theme="TRADINGVIEW_WHITE")
    c2 = PriceChart(series_1day, symbol + "2", "1day", market_code, is_equity, *aspect_2, dpi=300,
                    chart_type='simple_close',
                    # color_theme="NINJATRADER_SLATE_DARK")
                    # color_theme="FREEDOM24_DARK_TEAL")
                    # color_theme="OSCILLOSCOPE_FROM_90s")
                    color_theme="TRADINGVIEW_WHITE")
    # c = PriceChart()
    c.make_chart()
    c2.make_chart()
