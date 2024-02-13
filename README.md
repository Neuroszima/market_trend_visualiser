# Market trend visualizer

This project aims to download stock market data from certain provider and archive it, which then can be 
used freely for some other purpose. These purposes might include following:

- [ ] Visualising the data in any form in matplotlib chart package - for example as a series of price changes 
(called price action) in time. This price change could be visualised as, for example, series of "market close"
prices, or for example as a japanese candlestick chart.
- [ ] visualisations can be carried on web browser directly (in the similar fashion to the one used by TradingView.com)
- [ ] Data can be used to gather statistics about different time periods
- [ ] Finally, application could be used to serve as offline/online-based machine learning data feed


### Services used

This application has 2 main modules - API-handling functions and db-based functions.
There is also a ``minor_modules`` collection, which holds other purposes, that are connected with financial markets, but
currently they do not serve any purpose in main application


##### API handler

API-handling functions serve as data download helpers from a single provider - TwelveData
They have pretty good connectivity (simple enough) and their data collection is pretty decent.
The only thing that limits it, is the throughput from API creators - 5000 datapoints per query.
Another roadblock is the free-account limiters. This includes restricted access to exotic markets 
(you may have the data from NYSE or NASDAQ, but you won't get i.e. Indonesian/Chinese stock markets), that
are restricted by "subscription" plans. 

This can be partially solved by using both endpoints per-mail - the Rapid-API host endpoint, and direct
endpoint provided by TwelveData themselves. The incentive here is to go around their limiter of 800 free tokens
per day, as well as 8 token per minute.

So far this has been solved in the ``api_key_switcher_()`` function buried inside ``api_functions`` module,
however so far only free tier keys are covered by this function (meaning it will calculate delays that should be 
applied to not trip the download mechanisms)


##### DB handler

DB handler provides you with ready-to-setup structure written in SQL. The import can be made
using PSQL database ``cmd`` tool, to directly prepare the structure, ready for downloading the data. 
Most of it came to fruition by using pgAdmin (creating tables and schemas) tool, as well as GPT-4 
(function definitions, triggers, initial views)

There are some helper database functions, like the one that automatically creates database view for given time-series
(whether it would be "1-minute" or "1-day", DB structure has schemas for both).

These timeseries can then be used for visualisation purposes.


##### minor modules

Currently, there are some simple simulations in regard to "simple risk" and "reverse-martingale"
investing systems. Simulation generates several hundred randomised wallet balance trackers, that could give some basic 
idea on how such investing system works. 


### Application goals

:heavy_check_mark: handle downloading data through both endpoints (Rapid-API and direct TwelveData)

:heavy_check_mark: solve the problem of errors happening when data is downloaded too fast.

:heavy_check_mark: prepare a function to automatically download full timeseries from the earliest possible 
timestamp available

:heavy_minus_sign: prepare a procedure to fill the database with miscellaneous data from data provider (like: timezones, 
subscription plans, stock markets symbols, currency pair symbols, company symbols (per stock market), and other...)

:heavy_check_mark: elastic ability to use multiple keys, mixed between both endpoints. Using keys is now made through 
key permission lists that prevent quick token depletion, on the other enabling balanced downloads through threading

:heavy_check_mark: 1 minute and 1 day timeseries download calculates automatically how many tokens should be used

:heavy_minus_sign: extend saving time series to forex data (additional schema and modification to download functions)

:heavy_minus_sign: prepare automatic creation script for creating a fresh database, which is initially 
populated with a couple preselected stocks (kind of like "demo mode")

:heavy_minus_sign: prepare simple data visualizer for already downloaded timeseries 
(limited one - latest 90 days or latest 90 minutes)

:heavy_minus_sign: prepare a visual tool (very basic, for example in T-kinter) to mark which tickers have to be 
kept track of automatically when running script

:heavy_minus_sign: automate downloading data of certain market tickers and forex, when running script on Windows/Linux
computer (for example a script that you can run through Windows/Linux scheduler)

:heavy_minus_sign: track how many tokens have been used so far in a day on a given key
