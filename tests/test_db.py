import unittest
from datetime import datetime, timedelta
from random import choices, randint, random

import psycopg2

import db_functions.db_helpers as helpers
from db_functions.time_series_db import _drop_time_table, _drop_forex_table
import db_functions
import tests.t_helpers as t_helpers


class DBTests(unittest.TestCase):

    def setUp(self) -> None:
        db_functions.purge_db_structure()
        db_functions.import_db_structure()
        self.time_series_table_cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]

    def assertDatabaseHasRows(self, schema_name, table_name, correct_num_of_rows):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._table_rows_quantity.format(schema=schema_name, table_name=table_name))
            result = cur.fetchall()
            self.assertEqual(correct_num_of_rows, result[0][0])

    def assertTableExist(self, table_name: str, schema_name: str):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._information_schema_table_check.format(
                table_name=db_functions.db_string_converter(table_name),
                schema=db_functions.db_string_converter(schema_name),
            ))
            result = cur.fetchall()
            self.assertTrue(result)

    def assertViewExists(self, view_name: str, schema_name: str):
        self.assertTrue(db_functions.view_exists(view_name=view_name, schema_name=schema_name))

    def prepare_table_for_case(self, symbol: str, time_interval: str, is_equity: bool, mic: str, inserted_rows=1):
        """prepare a series of operations for testing database functions"""
        db_functions.create_time_series(symbol, time_interval, is_equity, mic_code=mic)
        dummy_data = t_helpers.generate_random_time_sample(time_interval, is_equity, span=inserted_rows)
        db_functions.insert_historical_data(dummy_data, symbol, time_interval, is_equity=is_equity, mic_code=mic)
        return dummy_data

    def save_forex_sample(self):
        """
        save a sample of data obtained from API

        there is a need for this set of instructions to be separate function because couple other
        tests rely on the input data that gets set by this function to work properly
        """
        sample_data = [
            {'symbol': 'AED/BRL', 'currency_group': 'Exotic-Cross', 'currency_base': 'UAE Dirham',
             'currency_quote': 'Brazil Real'},
            {'symbol': 'JPY/ZAR', 'currency_group': 'Exotic-Cross', 'currency_base': 'Japanese Yen',
             'currency_quote': 'South African Rand'},
            {'symbol': 'ARS/USD', 'currency_group': 'Exotic', 'currency_base': 'Argentinian Peso',
             'currency_quote': 'US Dollar'},
            {'symbol': 'KGS/RUB', 'currency_group': 'Minor', 'currency_base': 'Kyrgyzstan som',
             'currency_quote': 'Russian Ruble'},
            {'symbol': 'USD/EUR', 'currency_group': 'Major', 'currency_base': 'US Dollar',
             'currency_quote': 'Euro'},
            {'symbol': 'USD/GBP', 'currency_group': 'Major', 'currency_base': 'US Dollar',
             'currency_quote': 'British Pound'},
            {'symbol': 'USD/JPY', 'currency_group': 'Major', 'currency_base': 'US Dollar',
             'currency_quote': 'Japanese Yen'}
        ]

        currencies__ = set()
        currency_groups__ = set()

        # print(data[0])
        for forex_pair_data in sample_data:
            # currencies
            currency_symbols: list = forex_pair_data['symbol'].split('/')
            currency_base_symbol = currency_symbols[0]
            currency_quote_symbol = currency_symbols[1]
            currency_base_entry = {
                'name': forex_pair_data['currency_base'],
                'symbol': currency_base_symbol
            }
            currency_quote_entry = {
                'name': forex_pair_data['currency_quote'],
                "symbol": currency_quote_symbol,
            }
            currencies__.update((str(currency_base_entry),))
            currencies__.update((str(currency_quote_entry),))

            # groups
            currency_groups__.update((str(forex_pair_data['currency_group']),))

        try:
            db_functions.insert_forex_currency_groups(currency_groups__)
            db_functions.insert_currencies(currencies__)
            db_functions.insert_forex_pairs_available(sample_data)
        except psycopg2.Error as e:
            print(e)
            raise AssertionError("there was an error while saving correctly formatted data")
        return sample_data

    def save_markets_sample(self):
        """
        this procedure saves data about different stock markets

        as with case of forex, we need this function in couple different places
        """
        sample_data = [
            {'name': 'BHB', 'code': 'XBAH', 'country': 'Bahrain', 'timezone': 'Asia/Bahrain',
             'access': {'global': 'Level C', 'plan': 'Enterprise'}},
            {'name': 'SZSE', 'code': 'XSHE', 'country': 'China', 'timezone': 'Asia/Shanghai',
             'access': {'global': 'Level B', 'plan': 'Pro'}},
            {'name': 'LSE', 'code': 'XLON', 'country': 'United Kingdom', 'timezone': 'Europe/London',
             'access': {'global': 'Level A', 'plan': 'Grow'}},
            {'name': 'NASDAQ', 'code': 'XNGS', 'country': 'United States', 'timezone': 'America/New_York',
             'access': {'global': 'Basic', 'plan': 'Basic'}},
            {'name': 'NASDAQ', 'code': 'XNMS', 'country': 'United States', 'timezone': 'America/New_York',
             'access': {'global': 'Basic', 'plan': 'Basic'}},
            {'name': 'OTC', 'code': 'PINX', 'country': 'United States', 'timezone': 'America/New_York',
             'access': {'global': 'Basic', 'plan': 'Basic'}}
        ]

        plans_ = set()
        countries_ = set()
        timezones_ = set()

        for e in sample_data:
            access_obj = {
                'plan': e["access"]['plan'],
                "global": e["access"]['global'],
            }
            plans_.update((str(access_obj),))
            countries_.update((str(e['country']),))
            timezones_.update((str(e['timezone']),))

        try:
            db_functions.insert_timezones(timezones_)
            db_functions.insert_countries(countries_)
            db_functions.insert_plans(plans_)
            db_functions.insert_markets(sample_data)
        except psycopg2.Error as e:
            print(e)
            raise AssertionError("database error raised on proper insertion")
        return sample_data

    def save_equities_sample(self):
        """
        handy procedure for saving a couple of properly formatted stocks

        here samples are taken directly from API, that come from a query with option "show_plans"
        function MUST OBEY this format. No other is acceptable.
        """
        sample_data = [
            {'symbol': 'AADV', 'name': 'Albion Development VCT PLC', 'currency': 'GBp', 'exchange': 'LSE',
             'mic_code': 'XLON', 'country': 'United Kingdom', 'type': 'Common Stock',
             'access': {'global': 'Level A', 'plan': 'Grow'}},
            {'symbol': 'AAPL', 'name': 'Apple Inc', 'currency': 'USD', 'exchange': 'NASDAQ',
             'mic_code': 'XNGS', 'country': 'United States', 'type': 'Common Stock',
             'access': {'global': 'Basic', 'plan': 'Basic'}},
            {'symbol': 'NVDA', 'name': 'NVIDIA Corp', 'currency': 'USD', 'exchange': 'NASDAQ',
             'mic_code': 'XNGS', 'country': 'United States', 'type': 'Common Stock',
             'access': {'global': 'Basic', 'plan': 'Basic'}},
            {'symbol': 'OTEX', 'name': 'Open Text Corp', 'currency': 'USD', 'exchange': 'NASDAQ',
             'mic_code': 'XNGS', 'country': 'United States', 'type': 'Common Stock',
             'access': {'global': 'Basic', 'plan': 'Basic'}},
            {'symbol': 'ABLLL', 'name': 'Abacus Life, Inc.', 'currency': 'USD', 'exchange': 'NASDAQ',
             'mic_code': 'XNMS', 'country': 'United States', 'type': 'Exchange-Traded Note',
             'access': {'global': 'Basic', 'plan': 'Basic'}},
            {'symbol': 'BKISF', 'name': 'ISHARES IV PLC', 'currency': 'USD', 'exchange': 'CTC',
             'mic_code': 'PINX', 'country': 'United States', 'type': 'ETF',
             'access': {'global': 'Basic', 'plan': 'Basic'}}
        ]

        equity_types = set()

        for e in sample_data:
            equity_types.update((str(e['type']),))
        try:
            db_functions.insert_investment_types(equity_types)
            db_functions.insert_stocks(sample_data)
        except psycopg2.Error as e:
            print(e)
            raise AssertionError("database error raised on proper insertion")
        return sample_data

    def save_samples_for_tests(self):
        """
        save all the samples prepared for db tests, for the purpose of these tests
        and preparations of testing more sophisticated functions
        """
        self.save_forex_sample()
        self.save_markets_sample()
        self.save_equities_sample()

    def test_save_forex_data(self):
        """
        test saving data based on a data structured identically to the one coming from JSON response from API

        sample data is taken directly from API results
        """
        # for now a simple check for rowcount at pre-determined data
        self.save_forex_sample()
        self.assertDatabaseHasRows('public', 'currencies', 10)
        self.assertDatabaseHasRows('public', 'forex_currency_groups', 4)  # this is all that TwelveData defines
        self.assertDatabaseHasRows('public', 'forex_pairs', 7)

    def test_save_markets(self):
        """
        test saving data based on a data structured identically to the one coming from JSON response from API

        here samples are taken directly from API, that come from a query with option "show_plans"
        function MUST OBEY this format. No other is acceptable. (additional "access" parameter in output)
        """
        # this can be independent from forex data saving
        self.save_markets_sample()
        self.assertDatabaseHasRows('public', "timezones", 4)
        self.assertDatabaseHasRows('public', "countries", 5)  # additional country of 'Unknown' is present in db
        self.assertDatabaseHasRows('public', "plans", 4)  # every single that TwelveData has in paid plans offer
        self.assertDatabaseHasRows('public', "markets", 6)

    def test_save_equities(self):
        """
        test saving equity data based on structure identical to the one coming from API JSON response
        """
        self.save_forex_sample()
        self.save_markets_sample()
        # following is NOT independent on previous 2
        data_saved = self.save_equities_sample()  # only this data will be checked
        self.assertDatabaseHasRows('public', 'investment_types', 3)
        self.assertDatabaseHasRows('public', 'stocks', 6)

    def test_is_stock(self):
        """look at stock checking functionality (database function)"""
        err_msg1 = "stock symbol not recognized after insertion attempt - %s"
        err_msg2 = "stock symbol falsely recognized - %s"
        stocks = [
            ("NVDA", True),
            ("OTEX", True),
            ("AAPL", True),
            ("XXXXXXXX", False),
            ("AADV", True),
            ("".join(choices("AWNGORESDZ", k=21)), False),
            ("USD/CAD", False),
        ]
        self.save_samples_for_tests()

        for stock_symbol, in_database in stocks:
            if in_database:
                self.assertTrue(db_functions.is_equity(stock_symbol), msg=err_msg1 % stock_symbol)
            else:
                self.assertFalse(db_functions.is_equity(stock_symbol), msg=err_msg2 % stock_symbol)

    def test_is_forex_pair(self):
        """look at forex pair checking functionality (database function)"""
        err_msg1 = "forex symbol not recognized after insertion attempt - %s"
        err_msg2 = "forex symbol falsely recognized - %s"
        stocks = [
            ("NVDA", False),
            ("OTEX", False),
            ("AAPL", False),
            ("XXXXXXXX", False),
            ("AADV", False),
            ("".join(choices("AWNGORESDZ", k=21)), False),
            ("USD/CAD", False),  # this one is false because it should not exist in db as a test sample member
            ('KGS/RUB', True),
            ('USD/EUR', True)
        ]
        self.save_samples_for_tests()

        for symbol, in_database in stocks:
            if in_database:
                self.assertTrue(db_functions.is_forex_pair(symbol), msg=err_msg1 % symbol)
            else:
                self.assertFalse(db_functions.is_forex_pair(symbol), msg=err_msg2 % symbol)

    def test_obtain_latest_timestamp_from_db(self):
        """obtain latest timestamp from certain timetable, for table update purpose"""
        # first - stock market timestamp
        self.save_samples_for_tests()
        stock_, market_identification_code_ = "OTEX", "XNGS"
        interval_ = "1min"

        db_functions.create_time_series(
            symbol=stock_, mic_code=market_identification_code_, time_interval=interval_)
        self.assertTableExist(
            table_name=f"{stock_}_{market_identification_code_}",
            schema_name=f"{interval_}_time_series",
        )

        historical_dummy_data = [
            {
                "datetime": "2020-03-24 09:37:00", "open": 3, "close": 5, "high": 8, "low": 1.50,
                "volume": 490,
            },
        ]
        db_functions.insert_historical_data(
            historical_dummy_data, symbol=stock_, mic_code=market_identification_code_, time_interval=interval_
        )
        with self.assertRaises(ValueError):  # market identifier should not be ignored when querying stock
            db_functions.time_series_latest_timestamp(symbol=stock_, time_interval=interval_)
        self.assertIsNotNone(db_functions.time_series_latest_timestamp(
            symbol=stock_, mic_code=market_identification_code_, time_interval=interval_))

        # second - check forex tables
        symbol = "USD/CAD"
        interval_ = "1min"

        # clean for this mini-test
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur_ = conn.cursor()
            cur_.execute(_drop_forex_table.format(
                time_interval=interval_,
                symbol=symbol,
            ))
        with self.assertRaises(db_functions.DataUncertainError):  # USD/CAD not in database as forex pair
            db_functions.create_time_series(symbol=symbol, time_interval=interval_)
        symbol = 'USD/EUR'
        db_functions.create_time_series(symbol=symbol, time_interval=interval_)

        historical_dummy_data = [
            {
                "datetime": "2020-03-24 09:37:00", "open": 3, "close": 5, "high": 8, "low": 1.50,
            },
        ]
        db_functions.insert_historical_data(
            historical_dummy_data, symbol=symbol, time_interval=interval_)
        with self.assertRaises(ValueError):  # market identifier HAS TO be ignored when querying for currency pair
            db_functions.time_series_latest_timestamp(
                symbol=symbol, time_interval=interval_, mic_code=market_identification_code_)
        self.assertIsNotNone(db_functions.time_series_latest_timestamp(symbol, interval_))

    def test_equity_time_series_data_insertion(self):
        """test if properly formatted dummy data will be saved into database"""
        self.save_samples_for_tests()
        stock_, market_identification_code_ = "OTEX", "XNGS"
        intervals = ["1min", "1day"]

        for interval_ in intervals:
            schema_name = f"{interval_}_time_series"

            db_functions.create_time_series(
                symbol=stock_, mic_code=market_identification_code_, time_interval=interval_)
            self.assertTableExist(
                table_name=f"{stock_}_{market_identification_code_}",
                schema_name=schema_name,
            )
            total_dummy_data = t_helpers.generate_random_time_sample(interval_, True, span=randint(10, 20))
            historical_dummy_data = total_dummy_data[:len(total_dummy_data) // 2]
            historical_dummy_data2 = total_dummy_data[len(total_dummy_data) // 2:]
            print(interval_, total_dummy_data[0]['datetime'])
            # populate table with dummy data
            db_functions.insert_historical_data(
                historical_dummy_data, symbol=stock_, mic_code=market_identification_code_, time_interval=interval_
            )
            db_functions.insert_historical_data(
                historical_dummy_data2, symbol=stock_, mic_code=market_identification_code_, time_interval=interval_,
                rownum_start=len(historical_dummy_data),
            )
            table_name = stock_ + "_" + market_identification_code_
            assert db_functions.time_series_table_exists(stock_, interval_, mic_code=market_identification_code_)
            self.assertDatabaseHasRows(schema_name, table_name, len(total_dummy_data))
            # index should go from 0
            self.assertEqual(helpers.fetch_generic_last_ID_(
                schema_name, f"{stock_}_{market_identification_code_}"), len(total_dummy_data) - 1)

    def test_forex_time_series_data_insertion(self):
        """test if properly formatted dummy data will be saved into database"""
        self.save_forex_sample()
        symbol = "USD/GBP"
        schema_name = "forex_time_series"
        intervals = ["1min", "1day"]

        for interval_ in intervals:
            db_functions.create_time_series(symbol=symbol, time_interval=interval_)
            self.assertTableExist(
                table_name="_".join(symbol.split("/")).upper() + f"_{interval_}",
                schema_name=schema_name,
            )

            # populate table with dummy data
            total_dummy_data = t_helpers.generate_random_time_sample(interval_, True, span=randint(10, 20))
            historical_dummy_data = total_dummy_data[:len(total_dummy_data) // 2]
            historical_dummy_data2 = total_dummy_data[len(total_dummy_data) // 2:]
            db_functions.insert_historical_data(
                historical_dummy_data, symbol=symbol, time_interval=interval_, is_equity=False)
            db_functions.insert_historical_data(
                historical_dummy_data2, symbol=symbol, time_interval=interval_,
                rownum_start=len(historical_dummy_data), is_equity=False
            )
            table_ = "_".join(symbol.split("/")).upper() + f"_{interval_}"
            self.assertDatabaseHasRows("forex_time_series", table_, len(total_dummy_data))
            # following 'exists' function now rely on an internal db symbol search, opposed to us giving it is_stock
            assert db_functions.time_series_table_exists(symbol, interval_)
            self.assertEqual(helpers.fetch_generic_last_ID_(
                schema_name, table_name=table_), len(total_dummy_data) - 1)

    def test_create_financial_view(self):
        """test setting up financial views for different types of time series, as well as different timeframes"""
        # prepare the database with dummy data
        self.save_samples_for_tests()
        for symbol, time_interval, mic, is_equity in self.time_series_table_cases:
            schema_name = f"{time_interval}_time_series" if is_equity else "forex_time_series"
            table_name = f"{symbol}_{mic}" if is_equity else "%s_%s_%s" % (*symbol.split("/"), time_interval)

            # no table yet - error
            with self.assertRaises(db_functions.TimeSeriesNotFoundError):
                db_functions.create_time_series_view(symbol, time_interval, mic_code=mic)

            db_functions.create_time_series(symbol, time_interval, mic_code=mic)
            db_functions.create_time_series_view(symbol, time_interval, mic_code=mic)
            self.assertViewExists(view_name=f"{table_name}_view", schema_name=schema_name)

    def test_resolve_schema_table_names(self):
        """
        test if the function that resolves schema_name and table_name works, based on the data that is present in db
        """
        self.save_samples_for_tests()
        error_cases = [
            (None, None, None, None),
            ("bla", "OAIWJDOn", "awdo", 'hrn'),
            ("AAAA", "1min", None, False),
            ("AAPL", "1h", "XNGS", False),
            ("USD/EUR", "1h", None, None),
        ]
        for case in error_cases:
            symbol, time_interval, mic_code, is_equity_prediction = case
            with self.assertRaises((db_functions.DataUncertainError, ValueError)):
                db_functions.resolve_time_series_location(symbol, time_interval, mic_code)

        for case in self.time_series_table_cases:
            symbol, time_interval, mic_code, is_equity_prediction = case
            schema_name_prediction, table_name_prediction, _ = t_helpers.form_test_essentials(
                symbol, time_interval, mic_code, is_equity_prediction)
            schema_name, table_name, is_equity = db_functions.resolve_time_series_location(
                symbol, time_interval, mic_code=mic_code)
            self.assertEqual(is_equity, is_equity_prediction, msg=f"{is_equity}")
            self.assertEqual(schema_name, schema_name_prediction, msg=f"{schema_name}")
            self.assertEqual(table_name, table_name_prediction, msg=f"{table_name}")


if __name__ == '__main__':
    unittest.main()
