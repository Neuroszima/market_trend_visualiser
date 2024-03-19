from datetime import datetime
from os.path import abspath
from typing import Literal

from matplotlib import pyplot as plt
# from matplotlib import animation
from matplotlib.animation import FuncAnimation, PillowWriter  # noqa
from matplotlib.axes import Axes
from matplotlib.axis import Axis
# from matplotlib.artist import Artist
from matplotlib.figure import Figure, SubFigure
# from matplotlib.lines import Line2D
from matplotlib.spines import Spine

from vis_functions.vis_helpers import chart_time_split, resolve_timeframe_name, get_time_series_labels
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

        # chart
        self.size_x = size_x
        self.size_y = size_y
        self.chart_splits = chart_time_split(self._series_timestamps, self.timeframe, self.size_x / self.size_y)
        self.title: str | None = None
        self.figure: Figure | None = None
        self.main_chart: Axes | None = None
        self.dpi = dpi
        self.color_theme = "DARK_TEAL_FREEDOM24" if color_theme is None else color_theme
        self.chart_type = "simple_close" if chart_type is None else chart_type
        self.main_font_size = CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT_SIZE"] * 250/self.dpi
        self.labelsize = (CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT_SIZE"]-0.75) * 250/self.dpi

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

    def prepare_chart_space(self) -> Figure:
        figure: Figure = plt.figure(
            constrained_layout=True, figsize=(self.size_x / self.dpi, self.size_y / self.dpi),
            facecolor=CHART_SETTINGS[self.color_theme]['CHART_BG'],
        )
        figure.subplots(
            nrows=1, ncols=1, subplot_kw={"facecolor": CHART_SETTINGS[self.color_theme]['CHART_BG']},
        )
        self.main_chart: Axes = figure.get_axes()[0]
        timeframe = resolve_timeframe_name(self.timeframe)
        self.title = f"{self.symbol}_{self.market_code}" if self.is_stock else f"{self.symbol}"
        self.title += ", " + f"{timeframe}"
        self.main_chart.spines: dict[str, Spine]  # noqa
        for e, spine in self.main_chart.spines.items():
            spine.set_linewidth(0.5)
            if e in ["left", 'top']:
                spine.set_visible(False)
            spine.set(color=CHART_SETTINGS[self.color_theme]["CHART_SMALL_SEPARATORS"])

        if len(self._series_timestamps) > 100:
            time_ticks_visible = False
        else:
            time_ticks_visible = True

        self.main_chart.set_title(
            self.title, loc="left", fontstyle='normal', fontweight='normal', fontvariant='small-caps',
            color=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
            fontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
            fontsize=self.main_font_size,
            fontstretch='ultra-condensed')

        self.main_chart.yaxis: Axis  # noqa
        self.main_chart.yaxis.set_tick_params(
            which="major", left=False, right=True, direction="in",
            color=CHART_SETTINGS[self.color_theme]["CHART_MAIN_SEPARATORS"],
            labelcolor=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
            labelsize=self.labelsize,
            labelfontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
            labelleft=False, labelright=True,
        )

        self.main_chart.xaxis: Axis  # noqa
        self.main_chart.xaxis.set_tick_params(
            which="major", direction="in", bottom=time_ticks_visible,
            color=CHART_SETTINGS[self.color_theme]["CHART_MAIN_SEPARATORS"],
            labelcolor=CHART_SETTINGS[self.color_theme]["CHART_TEXT_COLOR"],
            labelsize=self.labelsize,
            labelfontfamily=CHART_SETTINGS[self.color_theme]["CHART_TEXT_FONT"],
        )

        return figure

    def draw_simple_chart(self, which_series: SERIES_DEFINER | None = None):
        print('simple')
        line_splitting_spec = chart_time_split(self._series_timestamps, self.timeframe, self.size_x / self.size_y)
        Y_labels = get_time_series_labels(line_splitting_spec, self._series_timestamps, self.timeframe)
        X = [*range(len(self._series_timestamps))]
        if which_series:
            Y = getattr(self, f"_series_{which_series}s")
        else:
            Y = getattr(self, f"_series_{self.chart_type.split('_')[1]}s")
        self.main_chart.plot(
            X, Y, linewidth=0.7,
            color=CHART_SETTINGS[self.color_theme]['CHART_MAIN_PLOT']
        )
        self.main_chart.set_xticks(X, labels=Y_labels)

    def draw_candlestick_chart(self):
        pass

    def save(self, name: str | None = None):
        if self.figure:
            if name:
                self.figure.savefig(CHART_FILE_LOCATION + "\\" + name + ".png", dpi=self.dpi)
            else:
                self.figure.savefig(CHART_FILE_LOCATION + "\\" + self.symbol, dpi=self.dpi)

    def make_chart(self, mode: str | None = None, chart_name: str | None = None):
        self.figure = self.prepare_chart_space()
        if mode == "simple":
            self.draw_simple_chart()
        else:
            if "simple" in self.chart_type:
                self.draw_simple_chart()
            else:
                self.draw_candlestick_chart()
        # plt.show()
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
    series_1min = generate_random_time_sample("1min", is_equity=is_equity, span=400)  # 400
    series_1day = generate_random_time_sample("1day", is_equity=is_equity, span=68)  # 68
    c = PriceChart(series_1min, symbol, "1min", market_code, is_equity, *aspect_2, dpi=300,
                   color_theme="NINJATRADER_SLATE_DARK")
                   # color_theme="FREEDOM24_DARK_TEAL")
                   # color_theme="OSCILLOSCOPE_FROM_90s")
                   # color_theme="TRADINGVIEW_WHITE")
    c2 = PriceChart(series_1day, symbol+"2", "1day", market_code, is_equity, *aspect_2, dpi=300,
                    # color_theme="NINJATRADER_SLATE_DARK")
                    color_theme="FREEDOM24_DARK_TEAL")
                    # color_theme="OSCILLOSCOPE_FROM_90s")
                   # color_theme="TRADINGVIEW_WHITE")
    # c = PriceChart()
    c.make_chart()
    c2.make_chart()
