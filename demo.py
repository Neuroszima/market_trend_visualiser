from datetime import datetime, timedelta
from os.path import isdir
from os import mkdir

import psycopg2
from psycopg2.errors import UniqueViolation, DuplicateObject

import vis_functions
import api_functions
import db_functions
import full_procedures


def visuals_switcher():
    while True:
        yield "OSCILLOSCOPE_FROM_90s"
        yield "FREEDOM24_DARK_TEAL"
        yield "TRADINGVIEW_WHITE"
        yield "NINJATRADER_SLATE_DARK"


if not isdir(vis_functions.CHART_FILE_LOCATION):
    mkdir(vis_functions.CHART_FILE_LOCATION)


# prepare and fill the database with data that is downloaded from TwelveData provider (stock/forex info)
key_switcher = api_functions.api_key_switcher()
try:
    full_procedures.perpare_database()
except DuplicateObject:
    print('db structure already prepared')

try:
    full_procedures.fill_database(key_switcher)
except UniqueViolation:
    print('db has already been filled with details')


# download a few of example tickers - a few daily (stock/forex) and one minutely (stock)
minute_tickers = ['UWMC']
daily_tickers = ["XAU/USD", "USD/JPY", "AAPL", "NVDA"]


symbols_in_database = []
for tickers, timeframe in zip([minute_tickers, daily_tickers], ['1min', '1day']):
    # this downloads full time series available from provider
    for symbol in tickers:
        # all the companies are listed in NYSE or NASDAQ which are US based.
        # since in db we do not have a country name "US" we could use "%U%S%"
        # since Postgres handles "%" and fills details automatically
        # 'Basic' level at provider means it can be accessed with the use of 'freemium' api key
        stock_info = db_functions.fetch_stocks(
            symbol_like=symbol, country_name_like="%U%S%", access_plan_name_like='Basic')
        if stock_info:
            mic = stock_info[0][4]
        else:
            mic = None
        full_procedures.time_series_save(symbol, mic, time_interval=timeframe, key_switcher=key_switcher)
        symbols_in_database.append((symbol, timeframe, mic))
        print(f"database table for {symbol} {mic} {timeframe} prepared and filled with up-to-date data")


# prepare a visual representation of downloaded data
color_themes = visuals_switcher()
chart_size = 1920, 1080
chart_size_2 = 1200, 1200

for index, (symbol, timeframe, market_identification_code) in enumerate(symbols_in_database):
    if 'day' in timeframe:
        timeseries_data = db_functions.fetch_data_by_dates(
            symbol, time_interval=timeframe, mic_code=market_identification_code,
            start_date=datetime(year=2022, month=3, day=20), time_span=timedelta(days=60)
        )
    else:
        timeseries_data = db_functions.fetch_data_by_dates(
            symbol, time_interval=timeframe, mic_code=market_identification_code,
            start_date=datetime(year=2022, month=3, day=20, hour=12, minute=30),
            trading_time_span=60
        )

    for i in range(4):
        c_scheme = next(color_themes)
        chart = vis_functions.PriceChart(
            timeseries_data, symbol, timeframe, market_identification_code,
            db_functions.is_equity(symbol), *chart_size, color_theme=c_scheme,  # noqa
            chart_type='candlestick'
        )
        chart_2 = vis_functions.PriceChart(
            timeseries_data, symbol, timeframe, market_identification_code,
            db_functions.is_equity(symbol), *chart_size_2, color_theme=c_scheme,  # noqa
            chart_type='candlestick'
        )

        # png is default output and can't be changed unless modified in project files
        chart.make_chart(chart_name="demo_{}_chart_{}".format(i, symbol.replace(chr(47), '_')))
        chart_2.make_chart(chart_name="demo_2_{}_chart_{}".format(i, symbol.replace(chr(47), '_')))
        print(f"chart for {symbol} {market_identification_code} {timeframe} prepared")
