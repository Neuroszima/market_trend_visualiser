import unittest
from random import choices

import psycopg2

import db_functions.db_helpers as helpers
from db_functions.time_series_db import _drop_time_table, _drop_forex_table
import db_functions


class DBTests(unittest.TestCase):

    def setUp(self) -> None:
        db_functions.purge_db_structure()
        db_functions.import_db_structure()

    def assertDatabaseHasRows(self, schema_name, table_name, correct_num_of_rows):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._table_rows_quantity.format(schema=schema_name, table_name=table_name))
            result = cur.fetchall()
            self.assertEqual(result[0][0], correct_num_of_rows)

    def assertTableExist(self, table_name, schema_name):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._information_schema_table_check.format(
                table_name=db_functions.db_string_converter(table_name),
                schema=db_functions.db_string_converter(schema_name),
            ))
            result = cur.fetchall()
            self.assertTrue(result)

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

    def save_markets(self):
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

    def save_equities(self):
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

    def test_save_forex_data(self):
        """
        test saving data based on a data structured identically to the one coming from JSON response from API

        sample data is taken directly from API results
        """
        self.save_forex_sample()

    def test_save_markets(self):
        """
        test saving data based on a data structured identically to the one coming from JSON response from API

        here samples are taken directly from API, that come from a query with option "show_plans"
        function MUST OBEY this format. No other is acceptable. (additional "access" parameter in output)
        """
        self.save_markets()

    def test_save_equities(self):
        """
        test saving equity data based on structure identical to the one coming from API JSON response
        """
        self.save_forex_sample()
        self.save_markets()
        self.save_equities()

    def test_is_stock(self):
        """look at stock checking functionality (database function)"""
        err_msg1 = "stock not recognized after insertion attempt - %s"
        err_msg2 = "stock falsely recognized - %s"
        stocks = [
            ("NVDA", True),
            ("OTEX", True),
            ("AAPL", True),
            ("XXXXXXXX", False),
            ("AADV", True),
            ("".join(choices("AWNGORESDZ", k=21)), False),
            ("USD/CAD", False),
        ]

        self.save_forex_sample()
        self.save_markets()
        self.save_equities()

        for stock, in_database in stocks:
            if in_database:
                self.assertTrue(db_functions.is_stock(stock), msg=err_msg1 % stock)
            else:
                self.assertFalse(db_functions.is_stock(stock), msg=err_msg2 % stock)

    def test_obtain_latest_timestamp_from_db(self):
        """obtain latest timestamp from certain timetable, for table update purpose"""
        # first - stock market timestamp
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
        db_functions.insert_equity_historical_data(
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
        db_functions.create_time_series(symbol=symbol, time_interval=interval_, is_equity=False)

        historical_dummy_data = [
            {
                "datetime": "2020-03-24 09:37:00", "open": 3, "close": 5, "high": 8, "low": 1.50,
            },
        ]
        db_functions.insert_equity_historical_data(
            historical_dummy_data, symbol=symbol, time_interval=interval_, is_equity=False)
        with self.assertRaises(ValueError):  # market identifier HAS TO be ignored when querying for currency pair
            db_functions.time_series_latest_timestamp(
                symbol=symbol, time_interval=interval_, mic_code=market_identification_code_, is_equity=False)
        self.assertIsNotNone(db_functions.time_series_latest_timestamp(
            symbol, interval_, is_equity=False))

    def test_equity_time_series_save(self):
        """test if properly formatted dummy data will be saved into database"""
        stock_, market_identification_code_ = "OTEX", "XNGS"
        intervals = ["1min", "1day"]

        for interval_ in intervals:
            if 'min' in interval_ or 'h' in interval_:
                datetimes = ['2020-03-24 09:37:00', '2020-03-24 09:36:00']
            else:
                datetimes = ['2020-03-25', '2020-03-24']

            schema_name = f"{interval_}_time_series"

            db_functions.create_time_series(
                symbol=stock_, mic_code=market_identification_code_, time_interval=interval_)
            self.assertTableExist(
                table_name=f"{stock_}_{market_identification_code_}",
                schema_name=schema_name,
            )

            # populate table with dummy data
            historical_dummy_data = [
                {
                    "datetime": datetimes[0], "open": 3, "close": 5, "high": 8, "low": 1.50,
                    "volume": 490,
                },
                {
                    "datetime": datetimes[1], "open": 1, "close": 3, "high": 4, "low": 0.50,
                    "volume": 190,
                }
            ]
            db_functions.insert_equity_historical_data(
                historical_dummy_data, symbol=stock_, mic_code=market_identification_code_, time_interval=interval_
            )
            db_functions.insert_equity_historical_data(
                historical_dummy_data, symbol=stock_, mic_code=market_identification_code_, time_interval=interval_,
                rownum_start=2,
            )
            table_name = stock_ + "_" + market_identification_code_
            assert db_functions.time_series_table_exists(stock_, interval_, mic_code=market_identification_code_)
            self.assertDatabaseHasRows(schema_name, table_name, 4)
            # index should go from 0
            self.assertEqual(helpers.last_row_ID_(schema_name, f"{stock_}_{market_identification_code_}"), 3)

    def test_forex_time_series_save(self):
        """test if properly formatted dummy data will be saved into database"""
        symbol = "USD/CAD"
        interval_ = "1min"

        db_functions.create_time_series(symbol=symbol, time_interval=interval_, is_equity=False)
        self.assertTableExist(
            table_name="_".join(symbol.split("/")).upper() + f"_{interval_}",
            schema_name=f"forex_time_series",
        )

        # populate table with dummy data
        historical_dummy_data = [
            {
                "datetime": "2020-03-24 09:37:00",
                "open": 3, "close": 5, "high": 8, "low": 1.50,
            },
            {
                "datetime": "2020-03-24 09:36:00",
                "open": 1, "close": 3, "high": 4, "low": 0.50,
            }
        ]
        db_functions.insert_equity_historical_data(
            historical_dummy_data, symbol=symbol, time_interval=interval_, is_equity=False)
        db_functions.insert_equity_historical_data(
            historical_dummy_data, symbol=symbol, time_interval=interval_, rownum_start=2, is_equity=False
        )
        table_ = "_".join(symbol.split("/")).upper() + f"_{interval_}"
        self.assertDatabaseHasRows("forex_time_series", table_, 4)
        assert db_functions.time_series_table_exists(symbol, interval_, is_equity=False)


if __name__ == '__main__':
    unittest.main()
