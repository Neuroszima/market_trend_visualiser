from pprint import pprint
from time import perf_counter, sleep
from typing import Literal, Optional, Generator

from api_functions.API_URLS import *

from settings import rapid_api_keys, regular_api_keys, RAPIDAPI_HOST
import requests

JSON_RESPONSE = dict[Literal['data', 'status']]


def parse_get_response_(
        querystring_parameters: dict, api_key_pair: tuple, request_type: None | str = None,
        data_type: None | str = None, ) -> dict | str:
    """
    prepare a request and parse response from selected API endpoint

    :return: 'str' if 'csv' was passed as a 'data_type' argument, 'dict'['data', 'status'] when 'json'
    """
    # print(request_type)
    # print(querystring)

    # make sure request will look like this:
    # response = requests.get(url=EARLIEST_TIMESTAMP, params=querystring, headers=headers)

    match request_type:
        case "earliest_timestamp":
            endpoint = EARLIEST_TIMESTAMP_URL
        case "time_series":
            endpoint = TIME_SERIES_URL
        case "list indices":
            endpoint = LIST_OF_AVAILABLE_INDICES
        case "list pairs":
            endpoint = LIST_OF_AVAILABLE_FOREX_PAIRS
        case "list exchanges":
            endpoint = LIST_OF_AVAILABLE_EXCHANGES
        case "list stocks":
            endpoint = LIST_OF_STOCKS_SYMBOLS
        case _:
            if api_key_pair == "regular":
                endpoint = API_USAGE_URL
            else:
                raise ValueError(f"request type value is invalid: {request_type}")

    get_request = dict()
    if "rapid" in api_key_pair[0]:
        get_request['headers'] = {
            "X-RapidAPI-Key": api_key_pair[1],
            "X-RapidAPI-Host": RAPIDAPI_HOST,
        }
        api = RAPIDAPI_GLOBAL_API_URL
    elif "regular" in api_key_pair[0]:
        querystring_parameters['apikey'] = api_key_pair[1]
        api = GLOBAL_API_URL
    else:
        raise KeyError("no api provided to connect to, or wrong type of api passed as an argument")

    get_request['url'] = api + endpoint
    get_request["params"] = querystring_parameters
    pprint(get_request)
    response = requests.get(**get_request)
    match data_type:
        case "json":
            result = response.json()
        case "csv":
            result = response.text
        case __:
            raise KeyError("data type must be either \'csv\' or \'json\'")

    return result


def get_api_usage_(api_key_pair: tuple):
    """
    get the information about tokens used so far with the use of certain key

    rapidAPI solution does not allow for getting daily api usage, only direct TwelveData supports it
    """
    if "regular" in api_key_pair[0]:
        return parse_get_response_(dict(), data_type="json", api_key_pair=api_key_pair)
    raise ValueError(f"API key with identifier {api_key_pair[0]} does not connect to direct TwelveData API")


def get_all_equities_(
        api_key_pair: tuple, data_type: str, additional_params: dict | None = None) -> JSON_RESPONSE | str:
    """
    obtain a list of stocks with their corresponding info

    to get information whether subscription plan covers obtaining data about the given
    stock, use {"show_plan": True} in additional parameters

    :param api_key_pair: tuple with (key_identifier, api_key) to be used to gather data
    :param data_type: return as csv or json formatted data
    :param additional_params: parameters like "exchange", "currency", "mic_code", "type" and other that are
    provided by TwelveData API to narrow down the searched parameters (as filters)
    """
    if not additional_params:
        additional_params = dict()
    return parse_get_response_(
        additional_params, request_type="list stocks", data_type=data_type, api_key_pair=api_key_pair)


def get_all_currency_pairs_(
        api_key_pair: tuple, data_type: str, additional_params: dict | None = None) -> JSON_RESPONSE | str:
    """
    obtain a list of forex pairs. These can be historically viewed by downloading time series

    :param api_key_pair: tuple with (key_identifier, api_key) to be used to gather data
    :param data_type: return as csv or json formatted data
    :param additional_params: parameters like "delimiter", "currency_group", "currency_quote" and other that are
    provided by TwelveData API to narrow down the searched parameters (as filters)
    """
    if not additional_params:
        additional_params = dict()
    return parse_get_response_(
        additional_params, request_type="list pairs", data_type=data_type, api_key_pair=api_key_pair)


def get_all_exchanges_(
        api_key_pair: tuple, data_type: str, additional_params: dict | None = None) -> JSON_RESPONSE | str:
    """
    obtain a list of exchanges with their corresponding countries and other information

    :param api_key_pair: tuple with (key_identifier, api_key) to be used to gather data
    :param data_type: return as csv or json formatted data
    :param additional_params: parameters like "show_plans", "delimiter", "timezone" and other that are
    provided by TwelveData API to narrow down the searched parameters
    """
    if not additional_params:
        additional_params = dict()
    return parse_get_response_(
        additional_params, request_type="list exchanges", data_type=data_type, api_key_pair=api_key_pair)


def api_key_switcher_(permitted_keys: Optional[list[str]] = None):
    """
    Prepare a collection of usable API keys, and use them cyclically

    Even when you have 1 API key, function will determine correct amount of time to sleep, so that other
    download processes won't halt due to abnormally fast token usage. Collection is based on settings.py parameters

    You can pass just a few, and to use the rest elsewhere (for example to split the downloading responsibility
    between many threads that way) by passing a list of keys that should be switched between.
    This way a function will switch only between the ones necessary for the process.

    :param permitted_keys: list of keywords that are attached to keys that should be used by the program instance,
    for example ['regular1', 'rapid1']
    """
    # print([(n, k) for n, k in regular_api_keys.items()])
    keys_dict = {key_name: [key, False] for key_name, key in regular_api_keys.items()}
    for key_name, key in rapid_api_keys.items():
        keys_dict[key_name] = [key, False]
    if permitted_keys is None:  # None - all keys permitted, else make selection
        permitted_keys = [key_name for key_name in keys_dict]
    elif not permitted_keys:
        raise KeyError("No api key passed to switcher. Did you forget to choose correct one?")
    while True:
        start = perf_counter()
        for key_name in permitted_keys:
            yield key_name, keys_dict[key_name][0]
            keys_dict[key_name][1] = True
            pprint(keys_dict)
        if (time_passed := perf_counter() - start) < 7.8:  # 8 requests per minute check
            sleep(7.8 - time_passed)
        # keys have "clocked out" - they can be used again without danger of "too fast" error
        for key_name in permitted_keys:
            keys_dict[key_name][1] = False


if __name__ == '__main__':

    # mini tests - 2 possible ways of forming key tuples
    api_key_pair_ = ('rapid0', rapid_api_keys['rapid0'])
    # api_key_pair_ = ('regular0', regular_api_keys['rapid0'])
    querystring = {
        "symbol": "NVDA",
        "mic": "XNGS",
        "interval": "1min",
    }
    print(parse_get_response_(
        querystring,
        request_type='earliest_timestamp',
        data_type='json',
        api_key_pair=api_key_pair_,
    ))

    key_switcher: Generator = api_key_switcher_(["regular0"])
    print(len(get_all_exchanges_(next(key_switcher), data_type='json')['data']))
    print(len(get_all_equities_(next(key_switcher), data_type='json')['data']))
    print(len(get_all_currency_pairs_(next(key_switcher), data_type='json')['data']))
