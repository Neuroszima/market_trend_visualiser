from time import sleep, perf_counter
from typing import Generator
from unittest import TestCase

import api_functions
import api_functions.api_responses_structure as data_responses

from settings import rapid_api_keys, regular_api_keys


class APITests(TestCase):
    """
    test suite aimed to check API information retrieval
    """
    key_switcher: Generator = api_functions.api_key_switcher(["regular0", "rapid0"])

    def setUp(self) -> None:
        pass

    def assertConformsDataResponse(self, response_structure: dict[str, type | dict], response_example):
        """use one of the definitions of response structure, and compare it with actual response of the query"""
        for key, type_ in response_structure.items():
            if isinstance(type_, type):
                self.assertTrue(isinstance(response_example[key], type_))
            elif isinstance(type_, dict):  # a sub-structure in the response
                for sub_key, sub_type in response_structure[key].items():  # data_responses.exchanges
                    self.assertTrue(isinstance(response_example[key][sub_key], sub_type))

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
        """obtain a list of exchanges from the TwelveData"""
        exchanges = api_functions.get_all_exchanges(next(APITests.key_switcher), data_type='json')

        # check the structure of the response according to the definition
        exchange_example = exchanges[0]
        self.assertConformsDataResponse(data_responses.exchanges, exchange_example)

    def test_get_all_equities(self):
        """obtain list of tracked equities from """
        equities = api_functions.get_all_equities(next(APITests.key_switcher), data_type='json')

        # check the structure of the response according to the definition
        equity_example = equities[0]
        self.assertConformsDataResponse(data_responses.equities, equity_example)
        # print(len(['data']))

    def test_get_all_currency_pairs(self):
        """obtain list of tracked equities from """
        currency_pairs = api_functions.get_all_currency_pairs(next(APITests.key_switcher), data_type='json')

        # check the structure of the response according to the definition
        currency_example = currency_pairs[0]
        print(currency_example)
        self.assertConformsDataResponse(data_responses.forex_pairs, currency_example)


