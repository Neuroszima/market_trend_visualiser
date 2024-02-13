from typing import Callable

import api_functions.miscellaneous_api as miscellaneous_api
import api_functions.time_series_api as time_series_api

download_time_series: Callable = time_series_api.download_time_series_
download_equity_history: Callable = time_series_api.download_equity_history_
obtain_earliest_timestamp: Callable = time_series_api.obtain_earliest_timestamp_

get_api_usage: Callable = miscellaneous_api.get_api_usage_
get_all_equities: Callable = miscellaneous_api.get_all_equities_
get_all_exchanges: Callable = miscellaneous_api.get_all_exchanges_
get_all_currency_pairs: Callable = miscellaneous_api.get_all_currency_pairs_
parse_get_response: Callable = miscellaneous_api.parse_get_response_
api_key_switcher: Callable = miscellaneous_api.api_key_switcher_
