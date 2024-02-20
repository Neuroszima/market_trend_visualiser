from datetime import datetime
from time import sleep, perf_counter
from typing import Generator
import unittest

import api_functions
import api_functions.api_responses_structure as data_responses

from settings import rapid_api_keys, regular_api_keys


class APITests(unittest.TestCase):
    """
    test suite aimed to check API information retrieval
    """
    key_switcher: Generator = api_functions.api_key_switcher(["regular0", "rapid0"])

    def setUp(self) -> None:
        pass

    def assertConformsDataResponse(self, response_structure: dict[str, type | dict], response_example):
        """use one of the definitions of response structure, and compare it with actual response of the query"""
        for key, type_ in response_structure.items():
            # print(key, type_, response_example[key])
            if isinstance(type_, type):  # check the type of response
                self.assertTrue(isinstance(response_example[key], type_))
            elif isinstance(type_, dict):  # a sub-structure in the response
                for sub_key, sub_type in response_structure[key].items():
                    self.assertTrue(isinstance(response_example[key][sub_key], sub_type))
            elif isinstance(type_, tuple):
                if len(type_) == 2:
                    # case of timestamp in structure - assert correctly reading timestamp, using
                    # strptime format from second argument
                    if isinstance(type_[0], datetime) and isinstance(type_[1], str):
                        try:
                            datetime.strptime(response_example[key], type_[1])
                        except ValueError:
                            raise AssertionError(
                                'datetime is not the data type that is loaded through the function at key:', key
                            )

    def test_key_switching_functionality(self):
        """
        test if key switcher works as intended,

        This also tests if, when all keys have been depleted by the generator in the 8-second cycle,
        the 8-second period is awaited, since in free key case we have to space out the usage of tokens
        """
        key_list = []
        if rapid_api_keys:
            key_list.append([key_name for key_name, _ in rapid_api_keys.items()][0])
        if regular_api_keys:
            key_list.append([key_name for key_name, _ in regular_api_keys.items()][0])

        self.assertTrue(key_list)  # prevents tests without keys in settings
        key_switcher = api_functions.api_key_switcher(key_list)
        start = perf_counter()  # measure 8 second period
        keys_served = []
        for i, k in enumerate(key_switcher):
            if i >= len(key_list):
                end = perf_counter()
                print("time of key serving cycle:", end-start)
                self.assertGreater(
                    end - start, 7.75, "time gap between cycles of serving keys is too small which "
                                       "might cause problems with too frequent querrying")
                for key_name, key_tuple in zip(key_list, keys_served):
                    self.assertIn(key_name, key_tuple)
                break
            keys_served.append(k)

    def test_get_and_parse_response(self):
        """
        Test if both methods of querying API work,
        meaning if responses from TwelveData API and Rapid are processed correctly
        """
        # 2 possible endpoints used to form key tuples
        api_key_pair_ = ('rapid0', rapid_api_keys['rapid0'])
        api_key_pair_2 = ('regular0', regular_api_keys['regular0'])

        querystring = {
            "symbol": "NVDA",
            "mic": "XNGS",
            "interval": "1min",
        }

        # rapid
        api_response = api_functions.parse_get_response(
            querystring,
            request_type='earliest_timestamp',
            data_type='json',
            api_key_pair=api_key_pair_,
        )
        print(api_response)
        self.assertIsNotNone(api_response['datetime'])
        self.assertIsNotNone(api_response['unix_time'])

        # regular
        api_response = api_functions.parse_get_response(
            querystring,
            request_type='earliest_timestamp',
            data_type='json',
            api_key_pair=api_key_pair_2,
        )
        self.assertIsNotNone(api_response['datetime'])
        self.assertIsNotNone(api_response['unix_time'])
        sleep(8)  # wait to use keys for another test

    def test_get_all_exchanges(self):
        """obtain a list of exchanges from the TwelveData provider"""
        exchanges = api_functions.get_all_exchanges(next(APITests.key_switcher), data_type='json')

        # check the structure of the response according to the definition
        exchange_example = exchanges[0]
        self.assertConformsDataResponse(data_responses.exchanges, exchange_example)

    def test_get_all_equities(self):
        """obtain list of tracked equities from the TwelveData provider"""
        equities = api_functions.get_all_equities(next(APITests.key_switcher), data_type='json')

        # check the structure of the response according to the definition
        equity_example = equities[0]
        self.assertConformsDataResponse(data_responses.equities, equity_example)
        # print(len(['data']))

    def test_get_all_currency_pairs(self):
        """obtain list of tracked equities from the TwelveData provider"""
        currency_pairs = api_functions.get_all_currency_pairs(next(APITests.key_switcher), data_type='json')

        # check the structure of the response according to the definition
        currency_example = currency_pairs[0]
        print(currency_example)
        self.assertConformsDataResponse(data_responses.forex_pairs, currency_example)

    def test_get_api_usage(self):
        """obtain information about how many tokens have been used up already using regular API subscription"""
        api_key = None
        for i, k in enumerate(APITests.key_switcher):
            if i > 2:
                break
            if "regular" in k[0]:
                api_key = k
            else:
                continue
        usage_info_response = api_functions.get_api_usage(api_key)
        self.assertConformsDataResponse(data_responses.api_usage, usage_info_response)

    def test_get_earliest_timestamp(self):
        """obtain information about the earliest time series data point of a given ticker"""
        ticker, market_code = "NVDA", "XNGS"
        forex_pair = "USD/CAD"
        time_intervals = ['1day', '1min']
        for interval_ in time_intervals:
            ticker_timestamp = api_functions.obtain_earliest_timestamp(
                ticker, mic_code=market_code, time_interval=interval_, api_key_pair=next(APITests.key_switcher))

            forex_timestamp = api_functions.obtain_earliest_timestamp(
                forex_pair, time_interval=interval_, api_key_pair=next(APITests.key_switcher))
            for tmstmp in [ticker_timestamp, forex_timestamp]:
                self.assertConformsDataResponse(
                    getattr(data_responses, f"earliest_timestamp_{interval_}"), tmstmp)


if __name__ == '__main__':
    unittest.main()
