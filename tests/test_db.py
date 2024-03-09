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
        self.assertDatabaseHasRows('public', "markets", 4)

    def test_save_equities(self):
        """
        test saving equity data based on structure identical to the one coming from API JSON response
        """
        self.save_forex_sample()
        self.save_markets_sample()
        # following is NOT independent on previous 2
        data_saved = self.save_equities_sample()  # only this data will be checked
        self.assertDatabaseHasRows('public', 'investment_types', 1)
        self.assertDatabaseHasRows('public', 'stocks', 4)

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
        self.save_samples_for_tests()

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
            self.assertEqual(helpers.last_row_ID_(
                schema_name, f"{stock_}_{market_identification_code_}"), len(total_dummy_data) - 1)

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
            assert db_functions.time_series_table_exists(symbol, interval_, is_equity=False)
            self.assertEqual(helpers.last_row_ID_(
                schema_name, table_name=table_), len(total_dummy_data) - 1)

    def test_create_financial_view(self):
        """test setting up financial views for different types of time series, as well as different timeframes"""
        # prepare the database with dummy data
        self.save_samples_for_tests()
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
        self.save_samples_for_tests()
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

    def test_locate_closest_datapoint(self):
        """test row locator function that is part of database data fetching functionality"""
        self.save_samples_for_tests()
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
            random_existing_point = randint(5, 15)
            random_existing_point2 = randint(25, 32)
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval,
                mic=mic, is_equity=is_equity, inserted_rows=45
            )

            # check edges of time series
            first_date = datetime.strptime(inserted_data[0]['datetime'], time_conversion)
            last_date = datetime.strptime(inserted_data[-1]['datetime'], time_conversion)
            impossible_date_1 = first_date - timedelta(days=40)
            impossible_date_2 = last_date + timedelta(days=40)
            # check when there is gap in the data and from the middle of the series
            middle_date_m = datetime.strptime(inserted_data[random_existing_point]['datetime'], time_conversion)

            sub_cases = [
                (first_date, first_date, '>=', None),
                (last_date, last_date, '<=', None),
                (impossible_date_1, first_date, '>=', None),
                (impossible_date_2, last_date, '<=', None),
                (impossible_date_1, impossible_date_1, '<=', db_functions.DataNotPresentError),
                (impossible_date_2, impossible_date_2, '>=', db_functions.DataNotPresentError),
                (middle_date_m, middle_date_m, '>=', None),
                (middle_date_m, middle_date_m, '<=', None),
            ]

            # if interval is day, due to repurposing the time sample generator
            # "middle_date" that should have been gap will be either Saturday or Sunday,
            # referencing Friday this week and Monday next week as target comp. dates
            if "day" in time_interval:
                random_weekend_day = datetime.strptime(
                    inserted_data[random_existing_point2]['datetime'], time_conversion)
                if (weekday := random_weekend_day.isoweekday()) in (1, 2, 3, 4, 5):
                    # in fact, not saturday or sunday...
                    random_weekend_day += timedelta(days=6 - weekday)  # make it at least saturday
                sub_cases.extend([
                    # Friday check -> "<="
                    (random_weekend_day,
                     random_weekend_day - timedelta(days=random_weekend_day.isoweekday() - 5),
                     '<=', None),
                    # next Monday -> ">="
                    (random_weekend_day,
                     random_weekend_day + timedelta(days=8 - random_weekend_day.isoweekday()),
                     ">=", None)
                ])

            # checks for each sub-case of current case
            for index, (reference_date, check_date, check_condition, raised_exception) in enumerate(sub_cases):
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
    def test_calculate_fetch_time_bracket(self):
        """
        test whether the amount (length of data to be fetched)
        and ID's of edges of the range that is about to be queried is ok
        """
        self.save_samples_for_tests()

        # too little inputs cases:
        missing_input_cases = [
            (None, None, None, None),
            (datetime.now(), None, None, None),
            (None, datetime.now(), None, None),
            (None, None, timedelta(days=randint(1, 30)), None),
            (None, None, None, randint(1, 200)),
        ]
        for missing_input_case in missing_input_cases:
            start_date, end_date, time_span, trading_time_span = missing_input_case
            with self.assertRaises(ValueError):
                # error should be raised prior to any schema/table correctness check
                db_functions.calculate_fetch_time_bracket(
                    "", "", start_date, end_date, time_span, trading_time_span)

        cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        for symbol, time_interval, mic, is_equity in cases:
            schema_name = f"{time_interval}_time_series" if is_equity else "forex_time_series"
            table_name = f"{symbol}_{mic}" if is_equity else "%s_%s_%s" % (*symbol.split("/"), time_interval)
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval,
                mic=mic, is_equity=is_equity, inserted_rows=45
            )
            sub_cases = []
            # (start of the data, end of the data)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'],
                ending_timestamp=inserted_data[0]['datetime_object']
            ))
            # (date long prior to start of the data, date long after end of the data)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(30, 90)),
                ending_timestamp=inserted_data[0]['datetime_object'] + timedelta(days=randint(30, 90))
            ))
            # (middle of the data, middle of the data later)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[randint(3, 10)]['datetime_object'],
                ending_timestamp=inserted_data[randint(23, 30)]['datetime_object']
            ))
            # (date prior to start of the data, date prior to start of the data)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(60, 70)),
                ending_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(10, 15)),
                raised_exception=db_functions.DataNotPresentError,
            ))
            # (date long after end of the data. date long after end of the data)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(10, 15)),
                ending_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(60, 70)),
                raised_exception=db_functions.DataNotPresentError,
            ))
            # (weekend day, weekend day of next week) (special case for 1day timeframe)
            if "day" in time_interval:
                prior_weekday = next((
                    day for day in t_helpers.generate_random_time_sample('1day', True, 45)
                    if day['datetime_object'].isoweekday() == 5))['datetime_object']
                prior_weekday += timedelta(days=1)
                next_weekday = prior_weekday + timedelta(days=randint(1, 2))
                sub_cases.extend(t_helpers.time_bracket_case_generator(
                    reference_dataset=inserted_data,
                    starting_timestamp=prior_weekday,
                    ending_timestamp=next_weekday,
                ))

            # these are variations of cases nr 2 above:
            # for example "give me data for 300 days timespan after this date", even when DB has like, 10 rows
            # this should not be punished imo, just return what you have up to most recent moment
            sub_cases.extend([
                (inserted_data[0]['datetime_object'], None, None,
                 len(inserted_data)*2, (0, len(inserted_data)-1), None),
                (inserted_data[0]['datetime_object'], None, timedelta(days=randint(300, 500)),
                 None, (0, len(inserted_data)-1), None),
                (None, inserted_data[-1]['datetime_object'], None,
                 len(inserted_data)*3, (0, len(inserted_data)-1), None),
                (None, inserted_data[-1]['datetime_object'], None,
                 len(inserted_data)*3, (0, len(inserted_data)-1), None),
            ])

            for sub_case in sub_cases:
                start_date, end_date, time_span, trading_time_span, predicted_answer, raised_exception = sub_case
                if raised_exception:
                    with self.assertRaises(raised_exception):
                        db_functions.calculate_fetch_time_bracket(
                            schema_name, table_name, start_date, end_date, time_span, trading_time_span
                        )

    @unittest.skip("this is a test stub")
    def test_get_data_from_view(self):
        pass

    @unittest.skip("this is a test stub")
    def test_view_structure(self):
        pass


if __name__ == '__main__':
    unittest.main()
