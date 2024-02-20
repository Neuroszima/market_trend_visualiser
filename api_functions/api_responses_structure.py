# all data types here only cover the "json to python" conversion
# these do not contain data types used in actual database
# for now these reflect types that should be obtained in certain API tests
from datetime import datetime


equities = {
    'symbol': str,
    'name': str,
    'currency': str,
    'exchange': str,
    'country': str,
    'type': str,
    'access': {
        'global': str,
        'plan': str
    }
}

# this contains dict in dict
exchanges = {
    'access': {
        'global': str,
        'plan': str
    },
    'code': str,
    'country': str,
    'name': str,
    'timezone': str
}

etfs = {

}

cryptocurrencies = {

}

forex_pairs = {
    'symbol': str,
    'currency_group': str,
    'currency_base': str,
    'currency_quote': str
}

# following contain proper read rules to be used in "strptime"
time_series_1min = {
    'datetime': (datetime, "%Y-%m-%d %H:%M:%S"),
    'high': float,
    'low': float,
    'open': float,
    'close': float,
    'volume': int
}

time_series_1day = {
    'datetime': (datetime, "%Y-%m-%d"),
    'high': float,
    'low': float,
    'open': float,
    'close': float,
    'volume': int
}

api_usage = {
    'timestamp': (datetime, "%Y-%m-%d %H:%M:%S"),
    'current_usage': int,
    'plan_limit': int,
    'daily_usage': int,
    'plan_daily_limit': int,
}

earliest_timestamp_1day = {
    'datetime': (datetime, "%Y-%m-%d"),
    'unix_time': int
}

earliest_timestamp_1min = {
    'datetime': (datetime, "%Y-%m-%d %H:%M:%S"),
    'unix_time': int
}

exchange_rate = {

}
