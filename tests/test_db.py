import unittest
from datetime import datetime, timedelta
from random import choices, randint, random

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

    def pop_row_from_database(self, day_to_pop_index, schema_name, table_name):
        """remove single row from time series or another table, based on the ID (p-key)"""
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._delete_single_based_on_ID.format(
                schema_name=schema_name,
                table_name=table_name,
                index=day_to_pop_index,
            ))

    def generate_random_time_sample(self, time_interval: str, is_equity: bool, rows: int = 10):
        """generate a couple of data points for given time series"""
        year = randint(2010, 2020)
        month = randint(2, 7)
        day = randint(1, 7)
        hour = randint(0, 3)
        minute = randint(2, 20)
        dates = [
            f"{year}-{month}-{day+i}" if "day" in time_interval else f"{year}-{month}-{day} {hour}:{minute+i}:00"
            for i in range(rows)
        ]
        candles = []
        for i in range(rows):
            bullish = 0.5 > random()
            # following order will be low, open, close, high; bullish default
            candle = sorted([randint(4, 26) for _ in range(4)])
            if not bullish:
                candle[1], candle[2] = candle[2], candle[1]  # swap open with close
            if is_equity:
                candle.append(randint(100, 300))
            candles.append(candle)
        data = [
            {
                "datetime": date,
                "low": candle_[0], "open": candle_[1],
                "close": candle_[2], "high": candle_[3],
            } for date, candle_ in zip(dates, candles)
        ]
        if is_equity:
            for point, candle in zip(data, candles):
                point['volume'] = candle[-1]
        return data

    def prepare_table_for_case(self, symbol: str, time_interval: str, is_equity: bool, mic: str, inserted_rows=1):
        """prepare a series of operations for testing database functions"""
        db_functions.create_time_series(symbol, time_interval, is_equity, mic_code=mic)
        dummy_data = self.generate_random_time_sample(time_interval, is_equity, rows=inserted_rows)
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
        db_functions.create_time_series(symbol=symbol, time_interval=interval_, is_equity=False)

        historical_dummy_data = [
            {
                "datetime": "2020-03-24 09:37:00", "open": 3, "close": 5, "high": 8, "low": 1.50,
            },
        ]
        db_functions.insert_historical_data(
            historical_dummy_data, symbol=symbol, time_interval=interval_, is_equity=False)
        with self.assertRaises(ValueError):  # market identifier HAS TO be ignored when querying for currency pair
            db_functions.time_series_latest_timestamp(
                symbol=symbol, time_interval=interval_, mic_code=market_identification_code_, is_equity=False)
        self.assertIsNotNone(db_functions.time_series_latest_timestamp(
            symbol, interval_, is_equity=False))

    def test_equity_time_series_data_insertion(self):
        """test if properly formatted dummy data will be saved into database"""
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
            total_dummy_data = self.generate_random_time_sample(interval_, True, rows=randint(10, 20))
            historical_dummy_data = total_dummy_data[:len(total_dummy_data) // 2]
            historical_dummy_data2 = total_dummy_data[len(total_dummy_data) // 2:]

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
            self.assertEqual(helpers.last_row_ID_(
                schema_name, f"{stock_}_{market_identification_code_}"), len(total_dummy_data)-1)

    def test_forex_time_series_data_insertion(self):
        """test if properly formatted dummy data will be saved into database"""
        symbol = "USD/CAD"
        schema_name = "forex_time_series"
        intervals = ["1min", "1day"]

        for interval_ in intervals:
            db_functions.create_time_series(symbol=symbol, time_interval=interval_, is_equity=False)
            self.assertTableExist(
                table_name="_".join(symbol.split("/")).upper() + f"_{interval_}",
                schema_name=schema_name,
            )

            # populate table with dummy data
            total_dummy_data = self.generate_random_time_sample(interval_, True, rows=randint(10, 20))
            historical_dummy_data = total_dummy_data[:len(total_dummy_data)//2]
            historical_dummy_data2 = total_dummy_data[len(total_dummy_data)//2:]
            db_functions.insert_historical_data(
                historical_dummy_data, symbol=symbol, time_interval=interval_, is_equity=False)
            db_functions.insert_historical_data(
                historical_dummy_data2, symbol=symbol, time_interval=interval_,
                rownum_start=len(historical_dummy_data), is_equity=False
            )
            table_ = "_".join(symbol.split("/")).upper() + f"_{interval_}"
            self.assertDatabaseHasRows("forex_time_series", table_, len(historical_dummy_data)*2)
            assert db_functions.time_series_table_exists(symbol, interval_, is_equity=False)
            self.assertEqual(helpers.last_row_ID_(
                schema_name, table_name=table_), len(total_dummy_data)-1)

    def test_create_financial_view(self):
        """test setting up financial views for different types of time series, as well as different timeframes"""
        # prepare the database with dummy data
        self.save_forex_sample()
        self.save_markets()
        self.save_equities()
        cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        for symbol, time_interval, mic, is_equity in cases:
            schema_name = f"{time_interval}_time_series" if is_equity else "forex_time_series"
            table_name = f"{symbol}_{mic}" if is_equity else "%s_%s_%s" % (*symbol.split("/"), time_interval)

            # no table yet - error
            with self.assertRaises(db_functions.TimeSeriesNotFoundError):
                db_functions.create_financial_view(symbol, time_interval, is_equity, mic_code=mic)

            db_functions.create_time_series(symbol, time_interval, is_equity, mic_code=mic)
            db_functions.create_financial_view(symbol, time_interval, is_equity, mic_code=mic)
            self.assertViewExists(view_name=f"{table_name}_view", schema_name=schema_name)

    def test_get_datapoint_by_date(self):
        # prepare the database with dummy data
        self.save_forex_sample()
        self.save_markets()
        self.save_equities()
        cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        for symbol, time_interval, mic, is_equity in cases:
            if time_interval in ['1day']:
                time_conversion = '%Y-%m-%d'
            elif time_interval in ['1min']:
                time_conversion = '%Y-%m-%d %H:%M:%S'
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval,
                mic=mic, is_equity=is_equity
            )
            date_check = datetime.strptime(inserted_data[0]['datetime'], time_conversion)
            point = db_functions.get_datapoint(symbol, time_interval, date_check, is_equity, mic_code=mic)
            self.assertTrue(point)
            incorrect_date = date_check - timedelta(days=randint(1, 45))
            with self.assertRaises(helpers.DataNotPresentError_):
                db_functions.get_datapoint(symbol, time_interval, incorrect_date, is_equity, mic_code=mic)

    # @unittest.skip("this is a test stub")
    def test_locate_closest_datapoint(self):
        """test row locator function that is part of database data fetching functionality"""
        self.save_forex_sample()
        self.save_markets()
        self.save_equities()
        cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        for symbol, time_interval, mic, is_equity in cases:
            schema_name = f"{time_interval}_time_series" if is_equity else "forex_time_series"
            table_name = f"{symbol}_{mic}" if is_equity else "%s_%s_%s" % (*symbol.split("/"), time_interval)
            if time_interval in ['1day']:
                time_conversion = '%Y-%m-%d'
            elif time_interval in ['1min']:
                time_conversion = '%Y-%m-%d %H:%M:%S'
            day_to_pop_index = randint(5, 15)
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval,
                mic=mic, is_equity=is_equity, inserted_rows=20
            )

            # check edges of time series
            first_date = datetime.strptime(inserted_data[0]['datetime'], time_conversion)
            last_date = datetime.strptime(inserted_data[-1]['datetime'], time_conversion)
            impossible_date_1 = first_date - timedelta(days=40)
            impossible_date_2 = last_date + timedelta(days=40)
            # check when there is gap in the data and from the middle of the series
            middle_date_prev = datetime.strptime(inserted_data[day_to_pop_index-1]['datetime'], time_conversion)
            middle_date_m = datetime.strptime(inserted_data[day_to_pop_index]['datetime'], time_conversion)
            middle_date_next = datetime.strptime(inserted_data[day_to_pop_index+1]['datetime'], time_conversion)

            interval_delta = timedelta(days=1) if "day" in time_interval else timedelta(minutes=1)

            sub_cases = [
                (first_date, first_date, '>=', None),
                (last_date, last_date, '<=', None),
                (middle_date_prev, middle_date_prev, '>=', None),
                (middle_date_next, middle_date_next, '<=', None),
                (impossible_date_1, impossible_date_1, '<=', db_functions.DataNotPresentError),
                (impossible_date_2, impossible_date_2, '>=', db_functions.DataNotPresentError),
                (middle_date_m, middle_date_m, '>=', None),
                (middle_date_m, middle_date_m, '<=', None),
                (middle_date_m, middle_date_m + interval_delta, '>=', None),  # after removing the middle datapoint
                (middle_date_m, middle_date_m - interval_delta, '<=', None),
            ]

            # checks for each sub-case of current case
            for index, (reference_date, check_date, check_condition, raised_exception) in enumerate(sub_cases):
                if index == 8:
                    self.pop_row_from_database(day_to_pop_index, schema_name, table_name)

                if raised_exception is None:
                    point_id = db_functions.locate_closest_datapoint(
                        reference_date, schema_name, table_name, check_condition)
                else:
                    with self.assertRaises(
                            raised_exception, msg=f"{(reference_date, check_condition, raised_exception)}"):
                        db_functions.locate_closest_datapoint(
                            reference_date, schema_name, table_name, check_condition)
                    continue

                fetched_data = db_functions.get_point_raw_by_pk(point_id, table_name, schema_name)
                self.assertEqual(point_id, fetched_data[0])
                self.assertEqual(check_date, fetched_data[1])

    @unittest.skip("this is a test stub")
    def test_get_data_from_view(self):
        pass

    @unittest.skip("this is a test stub")
    def test_view_structure(self):
        pass


if __name__ == '__main__':
    unittest.main()
