from datetime import datetime
from os.path import abspath

from matplotlib import pyplot as plt
# from matplotlib import animation
from matplotlib.animation import FuncAnimation, PillowWriter    # noqa
from matplotlib.axes import Axes
# from matplotlib.artist import Artist
from matplotlib.figure import Figure, SubFigure
# from matplotlib.lines import Line2D
# from matplotlib.spines import Spine

from vis_functions.vis_helpers import chart_split_lines


CHART_FILE_LOCATION = "\\".join(abspath(__file__).split("\\")[:-2] + ["charts"])


class PriceChart:
    """
    graphically present financial data from provider

    data input can be a directly downloaded data, or data coming from database fetch request
    """
    def __init__(self, data: list[tuple] | list[dict], symbol: str, timeframe: str, market_code: str,
                 is_stock: bool, size_x: int, size_y: int,
                 dpi: int | None = None):
        # disassemble data for various purposes
        # for example we could "show close" only, or draw entire candlesticks
        self.timeframe = timeframe
        self.market_code = market_code
        self.is_stock = is_stock
        self.symbol = symbol
        self._series_timestamps: list[datetime] | None = None
        self._series_lows: list[float | int] | None = None
        self._series_highs: list[float | int] | None = None
        self._series_opens: list[float | int] | None = None
        self._series_closes: list[float | int] | None = None
        self._series_volumes: list[int] | None = None
        self._disassemble(data, is_stock)

        # chart
        self.size_x = size_x
        self.size_y = size_y
        self.chart_splits = chart_split_lines(self._series_timestamps, self.timeframe)
        self.title: str | None = None
        self.figure: Figure | None = None
        self.main_chart: Axes | None = None
        self.dpi = dpi if dpi else 300

    def _disassemble(self, data: list[tuple | dict], is_stock: bool):
        """disassemble time series data into separate components for future reference"""
        # get to know if elements are dicts or tuples (provider origin or db origin)
        if isinstance(data[0], tuple):
            # data from database
            self._series_timestamps = [d[1] for d in data]
            self._series_lows = [d[1] for d in data]
            self._series_highs = [d[1] for d in data]
            self._series_opens = [d[1] for d in data]
            self._series_closes = [d[1] for d in data]
            if is_stock:
                self._series_volumes = [d[1] for d in data]
        elif isinstance(data[0], dict):
            # data downloaded
            self._series_timestamps = [d["datetime"] for d in data]
            self._series_lows = [d["low"] for d in data]
            self._series_highs = [d["high"] for d in data]
            self._series_opens = [d["open"] for d in data]
            self._series_closes = [d["close"] for d in data]
            if is_stock:
                self._series_volumes = [d["volume"] for d in data]
        else:
            raise TypeError('Unsupported data type for disassemble')

    def prepare_chart_space(self) -> Figure:
        figure: Figure = plt.figure(
            constrained_layout=True, figsize=(self.size_x/self.dpi, self.size_y/self.dpi))
        figure.subplots(nrows=1, ncols=1)
        self.main_chart: Axes = figure.get_axes()[0]
        self.title = f"{self.symbol}_{self.market_code}" if self.is_stock else f"{self.symbol}"
        self.main_chart.set_title(self.title)

        return figure

    def draw_simple_chart(self):
        pass

    def save(self):
        if self.figure:
            self.figure.savefig(CHART_FILE_LOCATION + "\\" + self.title, dpi=self.dpi)

    def make_chart(self):
        self.figure = self.prepare_chart_space()


if __name__ == '__main__':
    from random import seed
    from tests.t_helpers import generate_random_time_sample
    seed(2346346)

    symbol, timeframe, market_code, is_stock = "AAPL", "1min", "XNGS", True
    series_1min = generate_random_time_sample("1min", is_equity=True, span=25)  # sample means single candles here
    series_1day = generate_random_time_sample("1day", is_equity=True, span=25)
    c = PriceChart(series_1min, symbol, timeframe, market_code, is_stock, 1200, 1200, dpi=300)
    # c = PriceChart()
    c.make_chart()
    c.save()
