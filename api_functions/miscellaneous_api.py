# from pprint import pprint
import json
from datetime import datetime
from time import perf_counter, sleep
from typing import Literal, Optional, MutableMapping
from contextlib import suppress

from api_functions.API_URLS import *

from settings import rapid_api_keys, regular_api_keys, RAPIDAPI_HOST
import requests

JSON_RESPONSE = dict[Literal['data', 'status']]
RESPONSE_WITH_HEADERS = tuple[dict | str, MutableMapping]


def parse_get_response_(
        querystring_parameters: dict, api_key_pair: tuple, data_type: str, request_type: str) -> RESPONSE_WITH_HEADERS:
    """
    prepare a request and parse response from selected API endpoint

    :return: 'str' if 'csv' was passed as a 'data_type' argument, 'dict'['data', 'status'] when 'json'
    """
    # make sure request will look like this:
    # response = requests.get(url=EARLIEST_TIMESTAMP, params=querystring, headers=headers)
    match request_type:
        case "earliest_timestamp":
            querystring_parameters['timezone'] = "Europe/London"
            endpoint = EARLIEST_TIMESTAMP_URL
        case "time_series":
            querystring_parameters['timezone'] = "Europe/London"
            endpoint = TIME_SERIES_URL
        case "list indices":
            endpoint = LIST_OF_AVAILABLE_INDICES
        case "list pairs":
            endpoint = LIST_OF_AVAILABLE_FOREX_PAIRS
        case "list exchanges":
            endpoint = LIST_OF_AVAILABLE_EXCHANGES
        case "list stocks":
            endpoint = LIST_OF_STOCKS_SYMBOLS
        case "token_usage":
            endpoint = API_USAGE_URL  # proper key usage is handled in specified function
        case _:
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

    querystring_parameters['format'] = "CSV" if data_type == 'csv' else "JSON"
    get_request['url'] = api + endpoint
    get_request["params"] = querystring_parameters
    response = requests.get(**get_request)
    headers = response.headers
    match data_type:
        case "json":
            result: dict = response.json()
            if result.get('code') == 404:
                raise ConnectionError('Error with query:', result['message'])
        case "csv":
            result: str = response.text
            with suppress(ValueError):
                r_ = json.loads(result)
                if r_['code'] == 404:
                    raise ConnectionError('Error with query:', r_['message'])
        case __:
            raise KeyError("data type must be either \'csv\' or \'json\'")

    return result, headers


def get_api_usage_(api_key_pair: tuple):
    """
    get the information about tokens used so far with the use of certain key

    rapidAPI solution does not allow for getting daily api usage, only direct TwelveData supports it
    """
    if "regular" in api_key_pair[0]:
        return parse_get_response_(dict(), request_type="token_usage", data_type="json", api_key_pair=api_key_pair)[0]
    elif "rapid" in api_key_pair[0]:
        # form a response similar to regular api one, made from information obtained from headers
        _, headers = parse_get_response_(
            dict(), request_type="earliest_timestamp", data_type="json", api_key_pair=api_key_pair)
        return {
            'timestamp': datetime.strptime(headers['Date'], "%a, %d %b %Y %H:%M:%S GMT"),
            'current_usage': int(headers['Api-Credits-Used']),
            'plan_limit': int(headers['Api-Credits-Left']) + int(headers['Api-Credits-Used']),
            'daily_usage': int(headers['X-RateLimit-API-credits-Limit']) -
                           int(headers['X-RateLimit-API-credits-Remaining']),
            'plan_daily_limit': int(headers['X-RateLimit-API-credits-Limit'])
        }
    raise ValueError(f"API key with identifier {api_key_pair[0]} isn't valid key to use for ")


def get_all_equities_(
        api_key_pair: tuple, data_type: str, additional_params: dict | None = None) -> list[dict]:
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
        additional_params = {'show_plan': True}
    response_result, _ = parse_get_response_(
        additional_params, request_type="list stocks", data_type=data_type, api_key_pair=api_key_pair)
    return response_result['data']


def get_all_currency_pairs_(
        api_key_pair: tuple, data_type: str, additional_params: dict | None = None) -> list[dict]:
    """
    obtain a list of forex pairs. These can be historically viewed by downloading time series

    :param api_key_pair: tuple with (key_identifier, api_key) to be used to gather data
    :param data_type: return as csv or json formatted data
    :param additional_params: parameters like "delimiter", "currency_group", "currency_quote" and other that are
    provided by TwelveData API to narrow down the searched parameters (as filters)
    """
    if not additional_params:
        additional_params = dict()
    response_result, _ = parse_get_response_(
        additional_params, request_type="list pairs", data_type=data_type, api_key_pair=api_key_pair)
    return response_result['data']


def get_all_exchanges_(
        api_key_pair: tuple, data_type: str, additional_params: dict | None = None) -> list[dict]:
    """
    obtain a list of exchanges with their corresponding countries and other information

    :param api_key_pair: tuple with (key_identifier, api_key) to be used to gather data
    :param data_type: return as csv or json formatted data
    :param additional_params: parameters like "show_plans", "delimiter", "timezone" and other that are
    provided by TwelveData API to narrow down the searched parameters
    """
    if not additional_params:
        additional_params = {'show_plan': True}
    response_result, _ = parse_get_response_(
        additional_params, request_type="list exchanges", data_type=data_type, api_key_pair=api_key_pair)
    return response_result['data']


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
            # pprint(keys_dict)
        if (time_passed := perf_counter() - start) < 7.97:  # 8 requests per minute check
            sleep(7.97 - time_passed)
        # keys have "clocked out" - they can be used again without danger of "too fast" error
        for key_name in permitted_keys:
            keys_dict[key_name][1] = False
