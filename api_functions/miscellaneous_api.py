from typing import Literal

from api_functions.API_URLS import *

from settings import headers, regular_api_key
import requests

JSON_RESPONSE = dict[Literal['data', 'status']]


def parse_get_response(
        querystring_parameters: dict, request_type: None | str = None,
        data_type: None | str = None, api_type: None | str = None) -> dict | str:
    """
    prepare a request and parse response from selected API endpoint

    :return: 'str' if 'csv' was passed as a 'data_type' argument, 'dict'['data', 'status'] when 'json'
    """
    # print(request_type)
    # print(querystring)
    if api_type is None:
        api_type = "regular"
    if api_type == "regular":
        querystring_parameters['apikey'] = regular_api_key

    # make sure request will look like this:
    # response = requests.get(url=EARLIEST_TIMESTAMP, params=querystring, headers=headers)
    get_request = {
        "params": querystring_parameters
    }

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
            if api_type == "regular":
                endpoint = API_USAGE_URL
            else:
                raise ValueError(f"request type value is invalid: {request_type}")

    match api_type:
        case "rapid":
            get_request['headers'] = headers
            api = RAPIDAPI_GLOBAL_API_URL
        case "regular":
            api = GLOBAL_API_URL
        case _:
            raise KeyError("no api provided to connect to, or wrong type of api passed as an argument")

    get_request['url'] = api + endpoint
    # print(get_request)
    response = requests.get(**get_request)
    match data_type:
        case "json":
            result = response.json()
        case "csv":
            result = response.text
        case __:
            raise KeyError("data type must be either \'csv\' or \'json\'")

    return result


def get_api_usage():
    """rapidAPI solution does not allow for getting daily api usage"""
    return parse_get_response(dict(), data_type="json", api_type="regular")


def get_all_equities(
        api_type: str, data_type: str, additional_params: dict | None = None) -> JSON_RESPONSE | str:
    """
    obtain a list of stocks with their corresponding info

    to get information whether subscription plan covers obtaining data about the given
    stock, use {"show_plan": True} in additional parameters
    """
    if not additional_params:
        additional_params = dict()
    return parse_get_response(
        additional_params, request_type="list stocks", data_type=data_type, api_type=api_type)


def get_all_currency_pairs(
        api_type: str, data_type: str, additional_params: dict | None = None) -> JSON_RESPONSE | str:
    """obtain a list of forex pairs. These can be historically viewed by downloading time series"""
    if not additional_params:
        additional_params = dict()
    return parse_get_response(
        additional_params, request_type="list pairs", data_type=data_type, api_type=api_type)


def get_all_exchanges(
        api_type: str, data_type: str, additional_params: dict | None = None) -> JSON_RESPONSE | str:
    """obtain a list of exchanges with their corresponding countries and other information"""
    if not additional_params:
        additional_params = dict()
    return parse_get_response(
        additional_params, request_type="list exchanges", data_type=data_type, api_type=api_type)


if __name__ == '__main__':
    # print(get_api_usage())
    querystring = {
        "symbol": "NVDA",
        "mic": "XNGS",
        "interval": "1min",
    }
    print(parse_get_response(
        querystring,
        request_type='earliest_timestamp',
        data_type='json',
        api_type='rapid',
    ))
