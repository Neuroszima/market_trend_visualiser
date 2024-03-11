from datetime import datetime
from typing import Callable

import db_functions.forex_db as forex_db
import db_functions.markets_db as markets_db
import db_functions.stocks_db as stocks_db
import db_functions.time_series_db as time_series_db
import db_functions.sql_loader as sql_loader
import db_functions.db_helpers as db_helpers
import db_functions.db_views as db_views


insert_currencies: Callable = forex_db.insert_currencies_
insert_forex_pairs_available: Callable = forex_db.insert_forex_pairs_available_
insert_forex_currency_groups: Callable = forex_db.insert_forex_currency_groups_
fetch_currencies: Callable = forex_db.fetch_currencies_
fetch_forex_currency_groups: Callable = forex_db.fetch_forex_currency_groups_
fetch_forex_pairs: Callable = forex_db.fetch_forex_pairs_

insert_markets: Callable = markets_db.insert_markets_
insert_plans: Callable = markets_db.insert_plans_
insert_countries: Callable = markets_db.insert_countries_
insert_timezones: Callable = markets_db.insert_timezones_
fetch_plans: Callable = markets_db.fetch_plans_
fetch_markets: Callable = markets_db.fetch_markets_
fetch_countries: Callable = markets_db.fetch_countries_
fetch_timezones: Callable = markets_db.fetch_timezones_

insert_stocks: Callable = stocks_db.insert_stocks_
insert_investment_types: Callable = stocks_db.insert_investment_types_
fetch_investment_types: Callable = stocks_db.fetch_investment_types_
fetch_stocks: Callable = stocks_db.fetch_stocks_

create_time_series: Callable = time_series_db.create_time_series_
time_series_latest_timestamp: Callable[[str, str, bool, str | None], datetime] = \
    time_series_db.time_series_latest_timestamp_
time_series_table_exists: Callable = time_series_db.time_series_table_exists_
insert_historical_data: Callable = time_series_db.insert_historical_data_
fetch_datapoint_by_date: Callable = time_series_db.fetch_datapoint_by_date_
fetch_datapoint_raw_by_pk: Callable = time_series_db.fetch_datapoint_raw_by_pk_
locate_closest_datapoint: Callable = time_series_db.locate_closest_datapoint_
calculate_fetch_time_bracket: Callable = time_series_db.calculate_fetch_time_bracket_
fetch_data_by_dates: Callable = time_series_db.fetch_data_by_dates_

create_financial_view: Callable[[str, str, bool, str | None], None] = db_views.create_financial_view_
list_nonstandard_views: Callable[[], tuple] = db_views.list_nonstandard_views_
view_exists: Callable[[str], bool] = db_views.view_exists_

purge_db_structure: Callable = sql_loader.purge_db_structure_
import_db_structure: Callable = sql_loader.import_db_structure_

db_string_converter: Callable[[str], str] = db_helpers.db_string_converter_
list_nonstandard_functions: Callable = db_helpers.list_nonstandard_functions_
last_row_ID: Callable[[str, str], str] = db_helpers.last_row_ID_
fetch_data_raw_by_pks: Callable[[str, str], list] = db_helpers.fetch_data_raw_by_pks_
is_stock: Callable[[str], bool] = db_helpers.is_stock_
TimeSeriesNotFoundError: type[Exception] = db_helpers.TimeSeriesNotFoundError_
TimeSeriesExistsError: type[Exception] = db_helpers.TimeSeriesExistsError_
DataNotPresentError: type[Exception] = db_helpers.DataNotPresentError_
DataUncertainError: type[Exception] = db_helpers.DataUncertainError_
