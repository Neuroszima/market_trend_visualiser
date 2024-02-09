from datetime import datetime
from typing import Callable

import db_functions.forex_db as forex_db
import db_functions.markets_db as markets_db
import db_functions.stocks_db as stocks_db
import db_functions.time_series_db as time_series_db
import db_functions.purge_database as purge_database
import db_functions.setup_database as setup_database
import db_functions.db_helpers as db_helpers


insert_currencies: Callable = forex_db.insert_currencies_
insert_forex_pairs_available: Callable = forex_db.insert_forex_pairs_available_
insert_forex_currency_groups: Callable = forex_db.insert_forex_currency_groups_

insert_markets: Callable = markets_db.insert_markets_
insert_plans: Callable = markets_db.insert_plans_
insert_countries: Callable = markets_db.insert_countries_
insert_timezones: Callable = markets_db.insert_timezones_

insert_stocks: Callable = stocks_db.insert_stocks_
insert_investment_types: Callable = stocks_db.insert_investment_types_

create_time_series: Callable = time_series_db.create_time_series_
time_series_latest_timestamp: Callable[[str, str, str], datetime] = time_series_db.time_series_latest_timestamp_
time_series_table_exists: Callable = time_series_db.time_series_table_exists_
insert_equity_historical_data: Callable = time_series_db.insert_equity_historical_data_

purge_db_structure: Callable = purge_database.purge_db_structure_

import_db_structure: Callable = setup_database.import_db_structure_

db_string_converter: Callable = db_helpers.db_string_converter_
TimeSeriesNotFoundError: type[Exception] = db_helpers.TimeSeriesNotFoundError_
TimeSeriesExists: type[Exception] = db_helpers.TimeSeriesExists_
