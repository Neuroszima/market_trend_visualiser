# Market trend visualizer

This project aims to download stock market data from certain provider and archive it, which then can be 
used freely for some other purpose. These purposes might include following:

- [ ] Visualising the data in any form in matplotlib chart package - for example as a series of price changes 
(called price action) in time. This price change could be visualised as for example series of "market close"
prices, or for example as a japanese candlestick chart.
- [ ] visualisations can be carried on web explorer directly, serving the data for the browser
  (in the similar fashion to the one used by TradingView.com)
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

That is the reason you will see ``sleep(8)`` inside the actual code for time-series download

##### DB handler
DB handler provides you with ready-to-setup structure written in SQL. The import can be made
using PSQL database ``cmd`` tool, to directly prepare the structure, ready for downloading the data. 
Most of it came to fruition by using pgAdmin (creating tables and schemas) tool, as well as GPT-4 
(function definitions, triggers, initial views)

There are some helper database functions, like the one that automatically creates database view for given time-series
(whether it would be "1-minute" or "1-day", DB structure has schemas for both).

These timeseries can then be used for visualisation purposes.


##### minor modules

Currently there are some simple simulations in regard to "simple risk" and "reverse-martingale"
investing systems. Simulation generates several hundred randomised wallet balance trackers, that could give some basic 
idea on how such investing system works. 


### Application goals

- [x] handle downloading data through both endpoints (Rapid-API and direct TwelveData)
- [x] solve the problem of errors happening when data is downloaded too fast.
- [x] prepare a function to automatically download full timeseries from the earliest possible timestamp available
- [x] prepare a procedure to fill the database with miscellaneous data from data provider (like: timezones, 
subscription plans, stock markets symbols, currency pair symbols, company symbols (per stock market), and other...)
- [ ] track how many tokens have been used so far in a day
- [ ] prepare a visual tool (very basic, for example in T-kinter) to mark which tickers have to be kept track of 
automatically when running script
- [ ] automate downloading stock data of certain stock market tickers, when running script on windows computer (for 
example a cript that could be ran through windows/linux scheduler)
- [ ] prepare simple data visualizer for already downloaded timeseries (limited one - latest 90 days or latest 90 minutes)
- [ ] prepare automatic creation script for creating a fresh database
