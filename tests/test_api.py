from datetime import datetime, timedelta
from math import ceil
from pprint import pprint
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

    def assertExactSameKeysInDict(self, dict_a: dict, dict_b: dict):
        """check if dicts has exactly the same keys"""
        if not isinstance(dict_b, dict):
            raise ValueError("dict_b, not a dict")
        if not isinstance(dict_a, dict):
            raise TypeError("dict_a, not a dict")
        # comparing sets do not check the order, just if keys exist
        self.assertEqual(set([k for k, v in dict_a.items()]), set([k for k, v in dict_b.items()]))

    def assertConformsDataResponse(self, response_structure: dict[str, type | dict], response_example: dict):
        """use one of the definitions of response structure, and compare it with actual response of the query"""
        # check if there are EXACTLY the keys required for this response and nothing else
        self.assertExactSameKeysInDict(response_structure, response_example)
        for key, type_ in response_structure.items():
            # print(key, type_, response_example[key])
            if isinstance(type_, type):  # check the type of response
                self.assertTrue(isinstance(response_example[key], type_))
            elif isinstance(type_, dict):  # a sub-structure in the response
                self.assertExactSameKeysInDict(type_, response_structure[key])
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

    def assertInRangeInclusive(self, value, max, min):
        """assert value is within the range that includes values """
        assert min <= value <= max

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
        rapid_pair = ('rapid0', rapid_api_keys['rapid0'])
        regular_pair = ('regular0', regular_api_keys['regular0'])

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
            api_key_pair=rapid_pair,
        )
        print(api_response)
        self.assertIsNotNone(api_response['datetime'])
        self.assertIsNotNone(api_response['unix_time'])

        # regular
        api_response = api_functions.parse_get_response(
            querystring,
            request_type='earliest_timestamp',
            data_type='json',
            api_key_pair=regular_pair,
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
            if "regular" in k[0]:  # get the key that actually IS responsible with connecting to regular API
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

    def test_day_interval(self):
        """test pre-made with the help of ChatGPT"""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 12, 31)
        result = api_functions.calculate_iterations(start_date, '1day', end_date)
        expected_days = (end_date - start_date).days + 1
        expected_iterations = ceil((expected_days * 0.76) / 4999) + 1
        self.assertEqual(result, expected_iterations)

    def test_minute_interval_stocks(self):
        """test pre-made with the help of ChatGPT"""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 1, 2)
        result = api_functions.calculate_iterations(start_date, '1min', end_date, ask_stock=True)
        expected_days = (end_date - start_date).days + 1
        expected_iterations = ceil((expected_days * 0.76 * 390) / 4999) + 1
        self.assertEqual(result, expected_iterations)

    def test_minute_interval_forex(self):
        """test pre-made with the help of ChatGPT"""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 1, 2)
        result = api_functions.calculate_iterations(start_date, '1min', end_date, ask_stock=False)
        # personal edit - only 1 day of difference - at most 2 iterations needed, best would be 1
        # this is due to day having 1440 minutes and not needing more than 5k point download
        expected_iterations = 2
        self.assertEqual(result, expected_iterations)

    def test_without_end_date(self):
        """test pre-made with the help of ChatGPT"""
        start_date = datetime.now() - timedelta(days=100)
        result = api_functions.calculate_iterations(start_date, '1day')
        # This test's expected value is hard to assert due to dependency on the current date
        # but we can check if it does not raise an error and returns an int
        self.assertIsInstance(result, int)

    def test_invalid_time_interval(self):
        """test pre-made with the help of ChatGPT"""
        start_date = datetime(2020, 1, 1)
        with self.assertRaises(ValueError):
            api_functions.calculate_iterations(start_date, '7min')

    def test_dates_within_session_times(self):
        """test pre-made with the help of ChatGPT"""
        start = datetime(2023, 1, 1, 10, 0)
        end = datetime(2023, 1, 1, 15, 0)
        self.assertEqual(api_functions.preprocess_dates(start, end), (start, end))

    def test_dates_outside_session_times(self):
        """test pre-made with the help of ChatGPT"""
        start = datetime(2023, 1, 1, 8, 0)  # Before session open
        end = datetime(2023, 1, 1, 16, 0)  # After session close
        processed_start, processed_end = api_functions.preprocess_dates(start, end)
        self.assertEqual(processed_start, datetime(2023, 1, 1, 9, 30))
        self.assertEqual(processed_end, datetime(2023, 1, 1, 15, 59))

    def test_none_inputs(self):
        """test pre-made with the help of ChatGPT"""
        self.assertEqual(api_functions.preprocess_dates(None, None), (None, None))

    def test_mixed_dates(self):
        """test pre-made with the help of ChatGPT"""
        start = datetime(2023, 1, 1, 10, 0)  # Within session times
        end = datetime(2023, 1, 1, 16, 0)  # After session close
        processed_start, processed_end = api_functions.preprocess_dates(start, end)
        self.assertEqual(processed_start, start)
        self.assertEqual(processed_end, datetime(2023, 1, 1, 15, 59))

    def test_download_time_series(self):
        """
        test aims to download a set amount of points for both currency pair and equity
        and check correctness of data obtained
        """
        # following downloads should end with <5000 points
        start = datetime.strptime("2022-03-22 11:20", "%Y-%m-%d %H:%M")
        end = datetime.strptime("2022-03-25 10:20", "%Y-%m-%d %H:%M")
        start_daily = datetime.strptime("2022-03-22 11:20", "%Y-%m-%d %H:%M")
        end_daily = datetime.strptime("2022-05-25 10:20", "%Y-%m-%d %H:%M")
        series_example = api_functions.download_time_series(
            symbol="NVDA",
            api_key_pair=next(APITests.key_switcher),
            mic_code="XNGS",
            start_date=start,
            end_date=end,
            time_interval='1min'
        )
        self.assertConformsDataResponse(data_responses.equity_time_series_download_response, series_example)
        self.assertEqual(len(series_example['values']), 1111)

        series_example2 = api_functions.download_time_series(
            symbol="USD/CAD",
            api_key_pair=next(APITests.key_switcher),
            start_date=start,
            end_date=end,
            time_interval="1min"
        )
        diff = (end - start)
        assumed_datapoints_output = (diff.days * 24 * 60 + diff.seconds / 60) + 1
        self.assertConformsDataResponse(data_responses.forex_time_series_download_response, series_example2)
        # we cannot determine the real output of the database for 1 minute case in FX. There are
        # holes in data for 1 minute or so, and then those result in less then usual datapoints
        self.assertInRangeInclusive(
            len(series_example2['values']),
            assumed_datapoints_output,
            assumed_datapoints_output*0.9
        )

        series_daily = api_functions.download_time_series(
            symbol="NVDA",
            api_key_pair=next(APITests.key_switcher),
            mic_code="XNGS",
            start_date=start_daily,
            end_date=end_daily,
            time_interval='1day'
        )
        self.assertConformsDataResponse(data_responses.equity_time_series_download_response, series_daily)
        self.assertEqual(len(series_daily['values']), 45)

        series_daily2 = api_functions.download_time_series(
            symbol="USD/CAD",
            api_key_pair=next(APITests.key_switcher),
            start_date=start_daily,
            end_date=end_daily,
            time_interval="1day"
        )
        self.assertConformsDataResponse(data_responses.forex_time_series_download_response, series_daily2)
        self.assertEqual(len(series_daily2['values']), 46)


if __name__ == '__main__':
    unittest.main()
