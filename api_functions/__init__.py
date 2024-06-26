from typing import Callable

import api_functions.miscellaneous_api as miscellaneous_api
import api_functions.time_series_api as time_series_api

download_time_series: Callable = time_series_api.download_time_series_
download_market_ticker_history: Callable = time_series_api.download_market_ticker_history_
get_earliest_timestamp: Callable = time_series_api.get_earliest_timestamp_
get_latest_datapoint: Callable = time_series_api.get_latest_datapoint_
calculate_iterations: Callable = time_series_api.calculate_iterations_
preprocess_dates: Callable = time_series_api.preprocess_dates_

get_api_usage: Callable = miscellaneous_api.get_api_usage_
get_all_equities: Callable = miscellaneous_api.get_all_equities_
get_all_etfs: Callable = miscellaneous_api.get_all_etfs_
get_all_indices: Callable = miscellaneous_api.get_all_indices_
get_all_exchanges: Callable = miscellaneous_api.get_all_exchanges_
get_all_currency_pairs: Callable = miscellaneous_api.get_all_currency_pairs_
parse_get_response: Callable = miscellaneous_api.parse_get_response_
api_key_switcher: Callable = miscellaneous_api.api_key_switcher_
