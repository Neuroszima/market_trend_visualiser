from ast import literal_eval

from api_functions.API_URLS import (
    GLOBAL_API_URL, RAPIDAPI_GLOBAL_API_URL,
    TIME_SERIES_URL, EARLIEST_TIMESTAMP_URL, API_USAGE_URL
)

from settings import headers, regular_api_key
import requests


def recover_get_response(querystring: dict, request_type=None, data_type=None, api_type=None):
    # print(request_type)
    # print(querystring)
    if api_type is None:
        api_type = "regular"
    if api_type == "regular":
        querystring['apikey'] = regular_api_key

    # make sure request will look like this:
    # response = requests.get(url=EARLIEST_TIMESTAMP, params=querystring, headers=headers)
    get_request = {
        "params": querystring
    }

    match request_type:
        case "earliest_timestamp":
            endpoint = EARLIEST_TIMESTAMP_URL
        case "time_series":
            endpoint = TIME_SERIES_URL
        case _:
            endpoint = API_USAGE_URL

    match api_type:
        case "rapid":
            get_request['headers'] = headers
            api = RAPIDAPI_GLOBAL_API_URL
        case "regular":
            api = GLOBAL_API_URL
        case _:
            raise KeyError("no api provided to connect to")

    get_request['url'] = api + endpoint
    response = requests.get(**get_request)
    match data_type:
        case "json":
            result = literal_eval(response.text)
        case "csv":
            result = response.text
        case __:
            raise KeyError("data type must be either \'csv\' or \'json\'")

    return result


def get_api_usage():
    """rapidAPI solution does not allow for getting daily api usage"""
    return recover_get_response(dict(), data_type="json", api_type="regular")


if __name__ == '__main__':
    get_api_usage()
