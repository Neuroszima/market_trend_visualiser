import unittest
from datetime import datetime, timedelta
from random import choices, randint
from typing import Callable

import psycopg2
from psycopg2.errors import UndefinedTable

import db_functions.db_helpers as helpers
import db_functions
import tests.test_db as test_db
import tests.t_helpers as t_helpers


class DBFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_db = test_db.DBTests()
        db_functions.purge_db_structure()
        db_functions.import_db_structure()
        self.save_samples_for_tests()
        self.time_table_cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        self.public_tables = [
            ("public", "countries"),
            ("public", "markets"),
            ("public", "stocks"),
            ("public", "plans"),
            ("public", "forex_pairs"),
        ]

    def assertFetchCaseCompliant(
            self, fetch_filter_arguments: list | tuple, fetch_function: Callable, possible_result: list | type,
            list_of_database_contents: list):
        msg = f'failed: {fetch_filter_arguments} {fetch_function}'
        if isinstance(possible_result, type):  # exception
            with self.assertRaises(possible_result, msg=f'raising %s' % msg):  # noqa
                fetch_function(*fetch_filter_arguments)
            return
        fetch_result = fetch_function(*fetch_filter_arguments)
        self.assertEqual(len(possible_result), len(fetch_result), msg=f"case %s" % msg)
        if possible_result:
            for entry in possible_result:
                self.assertIn(list_of_database_contents[entry], fetch_result)

    def assertDatabaseAssumedRowsNumber(self, schema_name: str, table_name: str, data_length: int):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._table_rows_quantity.format(schema=schema_name, table_name=table_name))
            result = cur.fetchall()
            self.assertEqual(result[0][0], data_length)

    def assertCandleMatchesData(
            self, fetched_candle: tuple, data_candle: dict, time_string_conversion: str, is_equity: bool):
        try:
            self.assertEqual(
                datetime.strptime(data_candle['datetime'], time_string_conversion), fetched_candle[1])
            self.assertEqual(data_candle['open'], fetched_candle[2])
            self.assertEqual(data_candle['close'], fetched_candle[3])
            self.assertEqual(data_candle['high'], fetched_candle[4])
            self.assertEqual(data_candle['low'], fetched_candle[5])
            if is_equity:
                self.assertEqual(data_candle['volume'], fetched_candle[6])
        except AssertionError as e:
            print(e)
            raise AssertionError(f'candle do not match data:\n f={fetched_candle}\n d={data_candle}')

    def prepare_table_for_case(self, symbol: str, time_interval: str, is_equity: bool, mic: str, inserted_rows=1):
        """prepare a series of operations for testing database functions"""
        db_functions.create_time_series(symbol, time_interval, is_equity, mic_code=mic)
        dummy_data = t_helpers.generate_random_time_sample(time_interval, is_equity, span=inserted_rows)
        db_functions.insert_historical_data(dummy_data, symbol, time_interval, is_equity=is_equity, mic_code=mic)
        return dummy_data

    def save_samples_for_tests(self):
        """
        save all the samples prepared for db tests, for the purpose of these tests
        and preparations of testing more sophisticated functions
        """
        self.test_db.save_forex_sample()
        self.test_db.save_markets_sample()
        self.test_db.save_equities_sample()

    def test_get_datapoint_by_date(self):
        # prepare the database with dummy data
        cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        for symbol, time_interval, mic, is_equity in cases:
            _, __, time_conversion = t_helpers.form_test_essentials(symbol, time_interval, mic, is_equity)
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval, mic=mic, is_equity=is_equity)
            date_check = datetime.strptime(inserted_data[0]['datetime'], time_conversion)
            point = db_functions.fetch_datapoint_by_date(date_check, symbol, time_interval, is_equity, mic_code=mic)
            self.assertCandleMatchesData(point, inserted_data[0], time_conversion, is_equity)
            incorrect_date = date_check - timedelta(days=randint(1, 45))
            with self.assertRaises(helpers.DataNotPresentError_):
                db_functions.fetch_datapoint_by_date(incorrect_date, symbol, time_interval, is_equity, mic_code=mic)

    def test_locate_closest_datapoint(self):
        """test row locator function that is part of database data fetching functionality"""
        for symbol, time_interval, mic, is_equity in self.time_table_cases:
            schema_name, table_name, time_conversion = t_helpers.form_test_essentials(
                symbol, time_interval, mic, is_equity)
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
                    point_id = db_functions.fetch_ID_closest_to_date_(
                        reference_date, operation=check_condition, symbol=symbol, time_interval=time_interval,
                        is_equity=is_equity, mic_code=mic)
                else:
                    with self.assertRaises(
                            raised_exception, msg=f"{(reference_date, check_condition, raised_exception)}"):
                        db_functions.fetch_ID_closest_to_date_(
                            reference_date, operation=check_condition, symbol=symbol, time_interval=time_interval,
                            is_equity=is_equity, mic_code=mic)
                    continue

                fetched_data = db_functions.fetch_generic_by_ID_(point_id, table_name, schema_name)
                self.assertEqual(point_id, fetched_data[0])
                self.assertEqual(check_date, fetched_data[1])

    def test_calculate_fetch_time_bracket(self):
        """
        test whether the amount (length of data to be fetched)
        and ID's of edges of the range that is about to be queried is ok
        """
        # previous 2 cases removed because they cannot be formulated in equivalent way
        # also, cases are handled by 'retrieve schema...' functionality
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
            with self.assertRaises(LookupError, msg=f"function did not raise: {missing_input_case}"):
                # error should be raised prior to any schema/table correctness check
                db_functions.calculate_fetch_time_bracket(
                    "", "", is_equity=None, mic_code="", start_date=start_date,
                    end_date=end_date, time_span=time_span, trading_time_span=trading_time_span)

        for case in self.time_table_cases:
            symbol, time_interval, mic, is_equity = case
            schema_name, table_name, _ = t_helpers.form_test_essentials(symbol, time_interval, mic, is_equity)
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval,
                mic=mic, is_equity=is_equity, inserted_rows=25
            )
            sub_cases = []
            print(*(str(d['datetime']) + "\n" for d in inserted_data))
            # (start of the data, end of the data)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'],
                ending_timestamp=inserted_data[-1]['datetime_object']
            ))
            # (date long prior to start of the data, date long after end of the data)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(30, 90)),
                ending_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(30, 90))
            ))
            # (middle of the data, middle of the data later)
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[randint(3, 10)]['datetime_object'],
                ending_timestamp=inserted_data[randint(20, 24)]['datetime_object']
            ))
            # (date prior to start of the data, date prior to start of the data)
            c = t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(60, 70)),
                ending_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(10, 15)),
                raised_exception=db_functions.DataNotPresentError,
            )
            # subcase with "end date too early" and "trading span" should not be allowed
            # however, subcase with "start date too early" and "trading span" can be forgiven
            # justification -> "just give 20 earliest datapoints, i'll give you just some stupid early date"
            c.pop(3)  # 4th case in a list
            sub_cases.extend(c)
            # (date long after end of the data. date long after end of the data)
            c = t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(10, 15)),
                ending_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(60, 70)),
                raised_exception=db_functions.DataNotPresentError,
            )
            # simillar as above: "end date too late" and "trading span" combo can be allowed
            # justification -> "just give 20 latest datapoints, i'll give you just some stupid late date"
            c.pop()  # last in a list
            sub_cases.extend(c)
            # (weekend day, weekend day of next week) (special case for 1day timeframe)
            if "day" in time_interval:
                prior_weekday = next((
                    day for day in inserted_data if day['datetime_object'].isoweekday() == 5
                ))['datetime_object']
                prior_weekday += timedelta(days=1)
                next_weekday = prior_weekday + timedelta(days=7 + randint(1, 2))
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
                 len(inserted_data) * 3, (0, len(inserted_data) * 3 - 1), None),
                (inserted_data[0]['datetime_object'], None, timedelta(days=randint(300, 500)),
                 None, (0, len(inserted_data) - 1), None),
                (None, inserted_data[-1]['datetime_object'], timedelta(days=randint(300, 500)),
                 None, (0, len(inserted_data) - 1), None),
                (None, inserted_data[-1]['datetime_object'], None,
                 len(inserted_data) * 3, (0, len(inserted_data) - 1), None),
            ])

            for id_, sub_case in enumerate(sub_cases):
                start_date, end_date, time_span, trading_time_span, predicted_answer, raised_exception = sub_case
                message = f'test case: {id_=}, main:{case} sub: {sub_case}'
                print(message)
                if raised_exception:
                    with self.assertRaises(raised_exception, msg=message):
                        db_functions.calculate_fetch_time_bracket(
                            symbol, time_interval, is_equity, mic_code=mic, start_date=start_date,
                            end_date=end_date, time_span=time_span, trading_time_span=trading_time_span)
                    continue
                id_bracket_ = db_functions.calculate_fetch_time_bracket(
                    symbol, time_interval, is_equity, mic_code=mic, start_date=start_date,
                    end_date=end_date, time_span=time_span, trading_time_span=trading_time_span)
                self.assertEqual(predicted_answer, id_bracket_, msg=message)

    def test_fetch_currencies(self):
        currency_list = [
            (0, "Argentinian Peso", "ARS"), (1, "Brazil Real", "BRL"),
            (2, "British Pound", "GBP"), (3, "Euro", "EUR"), (4, "Japanese Yen", "JPY"),
            (5, "Kyrgyzstan som", "KGS"), (6, "Russian Ruble", "RUB"),
            (7, "South African Rand", "ZAR"), (8, "UAE Dirham", "AED"), (9, "US Dollar", "USD"),
        ]
        cases = [
            (None, None, [c[0] for c in currency_list]),
            ("", "", ValueError),
            ("", None, ValueError),
            (None, "", ValueError),
            ('E%', None, [3]),
            (None, 'U%', [8, 9]),
            (None, '%E%', [8, 3]),
            ('%R%', None, [0, 1, 3, 6, 7]),
            (None, 'Z', []),
            ("Z", None, []),
        ]
        for case in cases:
            self.assertFetchCaseCompliant(case[:-1], db_functions.fetch_currencies, case[-1], currency_list)

    def test_fetch_forex_currency_groups(self):
        groups_list = [
            (0, "Exotic"), (1, "Exotic-Cross"),
            (2, "Major"), (3, "Minor"),
        ]
        cases = [
            ('', ValueError),
            (None, [g[0] for g in groups_list]),
            ('Exotic', [0]),
            ('Exotic%', [0, 1]),
            ('M', []),
            ('M%', [2, 3]),
        ]
        for case in cases:
            self.assertFetchCaseCompliant([case[0]], db_functions.fetch_forex_currency_groups, case[-1], groups_list)

    def test_fetch_forex_pairs(self):
        """
        Test forex pairs available in database
        There are 6 different filters here, 2 currency symbols (quote, base), their respective names,
        the entire symbol of a pair (one with "/" in it) and the name of currency group
        """
        forex_pairs = [
            (0, 'Exotic-Cross', 'AED/BRL', 'AED', 'BRL', 'UAE Dirham', 'Brazil Real'),
            (1, 'Exotic', 'ARS/USD', 'ARS', 'USD', 'Argentinian Peso', 'US Dollar'),
            (2, 'Exotic-Cross', 'JPY/ZAR', 'JPY', 'ZAR', 'Japanese Yen', 'South African Rand'),
            (3, 'Minor', 'KGS/RUB', 'KGS', 'RUB', 'Kyrgyzstan som', 'Russian Ruble'),
            (4, 'Major', 'USD/EUR', 'USD', 'EUR', 'US Dollar', 'Euro'),
            (5, 'Major', 'USD/GBP', 'USD', 'GBP', 'US Dollar', 'British Pound'),
            (6, 'Major', 'USD/JPY', 'USD', 'JPY', 'US Dollar', 'Japanese Yen')
        ]
        cases = [
            ('M%', None, None, None, None, None, [3, 4, 5, 6]),
            (None, None, None, "%PY", None, None, [6]),
            (None, None, None, None, '%Dollar%', '%Pound%', [5]),
            (None, None, 'USD', None, None, None, [4, 5, 6]),
            (None, 'USD%', None, None, None, None, [4, 5, 6]),
            ('E%', None, None, None, None, None, [0, 1, 2]),
            ('Exotic', None, None, None, None, None, [1]),
            ("Major", None, None, "GBP", None, None, [5]),
            ("Major", None, None, None, "X Dollar", "Y peso", []),
            (*[None]*6, [fp[0] for fp in forex_pairs]),
            *[(*[field for field in forex_pairs[index][1:]], [index]) for index in range(len(forex_pairs))]
        ]
        cases.extend([([None if j != i else '' for j in range(6)] + [ValueError])  # noqa
                      for i in range(len(forex_pairs[0])-1)])
        for case in cases:
            self.assertFetchCaseCompliant(case[:-1], db_functions.fetch_forex_pairs, case[-1], forex_pairs)

    def test_fetch_plans(self):
        """
        For this test we only check the 'name' of the plan which is the 'plan' field in DB.
        This is the only parameter exposed to be filtered in the function.
        """
        plans = [
            (0, 'Basic', 'Basic'), (1, 'Level C', 'Enterprise'),
            (2, 'Level A', 'Grow'), (3, 'Level B', 'Pro')
        ]
        cases = [
            ('', ValueError),
            (None, [p[0] for p in plans]),
            ('%o%', [2, 3]),
            ('B%', [0]),
            ('E%', [1]),
            ('X', []),
            *[(*[field for field in plans[index][2:]], [index]) for index in range(len(plans))]
        ]
        for case in cases:
            self.assertFetchCaseCompliant([case[0]], db_functions.fetch_plans, case[-1], plans)

    def test_fetch_markets(self):
        markets = [
            (0, 'BHB', 'XBAH', 'Asia/Bahrain', 'Enterprise', 'Bahrain'),
            (1, 'LSE', 'XLON', 'Europe/London', 'Grow', 'United Kingdom'),
            (2, 'NASDAQ', 'XNGS', 'America/New_York', 'Basic', 'United States'),
            (3, 'NASDAQ', 'XNMS', 'America/New_York', 'Basic', 'United States'),
            (4, 'OTC', 'PINX', 'America/New_York', 'Basic', 'United States'),
            (5, 'SZSE', 'XSHE', 'Asia/Shanghai', 'Pro', 'China')
        ]
        cases = [
            ('NAS%', None, None, None, None, [2, 3]),
            (None, None, None, "Basic", None, [2, 3, 4]),
            ("B%", None, None, '%Enterprise%', None, [0]),
            (None, None, 'Asia%', None, None, [0, 5]),
            (None, 'X%', None, None, None, [0, 1, 2, 3, 5]),
            ('%E%', None, None, None, None, [1, 5]),
            ('E%', None, None, None, None, []),
            (None, None, "%New_York", None, None, [2, 3, 4]),
            (None, None, "%New York", None, None, []),  # they probably do not allow spaces in 2 segment names
            (None, None, None, None, "%States", [2, 3, 4]),
            ("Euroxnet", None, None, None, "%States", []),
            (*[None]*5, [fp[0] for fp in markets]),
            *[(*[field for field in markets[index][1:]], [index]) for index in range(len(markets))]
        ]
        cases.extend([([None if j != i else '' for j in range(5)] + [ValueError])  # noqa
                      for i in range(len(markets[0])-1)])
        for case in cases:
            print(case)
            self.assertFetchCaseCompliant(case[:-1], db_functions.fetch_markets, case[-1], markets)

    def test_fetch_countries(self):
        """Test countries available in database. Fetch filter is name of country"""
        countries = [
            (0, "Bahrain"), (1, "China"), (2, "United Kingdom"),
            (3, "United States"), (4, "Unknown"),
        ]
        cases = [
            ('', ValueError),
            (None, [p[0] for p in countries]),
            *[(*[field for field in countries[index][1:]], [index]) for index in range(len(countries))],
            ('%a%', [0, 1, 3]),
            ('B%', [0]),
            ('U%', [2, 3, 4]),
            ('United%', [2, 3]),
            ('%own', [4]),
            ('X', []),
        ]
        for case in cases:
            self.assertFetchCaseCompliant([case[0]], db_functions.fetch_countries, case[-1], countries)

    def test_fetch_timezones(self):
        """
        Test countries available in database. Fetch filter is name of country
        Technically we could use them to change the date timestamps of incoming time series data,
        but we only do have them for archive purposes and relational database things
        """
        timezones = [
            (0, "America/New_York"),
            (1, "Asia/Bahrain"),
            (2, "Asia/Shanghai"),
            (3, "Europe/London"),
        ]
        cases = [
            ('', ValueError),
            (None, [p[0] for p in timezones]),
            *[(*[field for field in timezones[index][1:]], [index]) for index in range(len(timezones))],
            ('%a%', [0, 1, 2]),
            ('Asia%', [1, 2]),
            ('U%', []),
            ('A%', [0, 1, 2]),
            ('E%', [3]),
            ('%Bahrain', [1]),
            ('X', []),
        ]
        for case in cases:
            self.assertFetchCaseCompliant([case[0]], db_functions.fetch_timezones, case[-1], timezones)

    def test_fetch_investment_types(self):
        types = [
            (0, "Common Stock"),
            (1, "ETF"),
            (2, "Exchange-Traded Note"),
        ]
        cases = [
            ('', ValueError),
            (None, [p[0] for p in types]),
            *[(*[field for field in types[index][1:]], [index]) for index in range(len(types))],
            ('%o%', [0, 2]),
            ('E%', [1, 2]),
            ('%Stock', [0]),
            ('ETF', [1]),
            ('X', []),
            ('x', []),
        ]
        for case in cases:
            self.assertFetchCaseCompliant([case[0]], db_functions.fetch_investment_types, case[-1], types)

    def test_fetch_stocks(self):
        stocks = [
            (0, 'AADV', 'Albion Development VCT PLC', 'GBP', 'XLON', 'United Kingdom', 'Common Stock', 'Grow'),
            (1, 'AAPL', 'Apple Inc', 'USD', 'XNGS', 'United States', 'Common Stock', 'Basic'),
            (2, 'ABLLL', 'Abacus Life, Inc.', 'USD', 'XNMS', 'United States', 'Exchange-Traded Note', 'Basic'),
            (3, 'BKISF', 'ISHARES IV PLC', 'USD', 'PINX', 'United States', 'ETF', 'Basic'),
            (4, 'NVDA', 'NVIDIA Corp', 'USD', 'XNGS', 'United States', 'Common Stock', 'Basic'),
            (5, 'OTEX', 'Open Text Corp', 'USD', 'XNGS', 'United States', 'Common Stock', 'Basic'),
        ]
        cases = [
            ('M%', None, None, None, None, None, None, []),
            (None, "%Inc%", None, None, None, None, None, [1, 2]),
            (None, "%PLC", None, None, None, None, None, [0, 3]),
            (None, None, 'USD', None, None, None, None, [1, 2, 3, 4, 5]),
            (None, None, 'GBP', None, None, None, None, [0]),
            (None, None, 'USD', 'X%', None, None, None, [1, 2, 4, 5]),
            (None, None, None, None, '%Kingdom', None, None, [0]),
            ('A%', None, None, None, None, "%Note", None, [2]),
            (None, None, None, None, None, "Common Stock", None, [0, 1, 4, 5]),
            (None, None, None, None, None, "ETF", None, [3]),
            (None, None, None, None, None, None, "Basic", [i+1 for i in range(5)]),
            (*[None]*7, [s[0] for s in stocks]),
            *[(*[field for field in stocks[index][1:]], [index]) for index in range(len(stocks))]
        ]
        cases.extend([([None if j != i else '' for j in range(7)] + [ValueError])  # noqa
                      for i in range(len(stocks[0])-1)])
        for case in cases:
            self.assertFetchCaseCompliant(case[:-1], db_functions.fetch_stocks, case[-1], stocks)

    def test_fetch_datapoint_by_date(self):
        """
        test fetching a single point of time from database by date
        other information consist of timeframe, stock symbol, market id code and such
        """

        # two rows with the same date should not happen!
        # TODO - suspended
        # bad user combination of inputs
        with self.assertRaises(db_functions.TimeSeriesNotFoundError):
            db_functions.fetch_datapoint_by_date(datetime.now(), "AAAPL", "1day", True, "XNGS")

        for symbol_, time_interval, mic, is_equity in self.time_table_cases:
            _, __, time_conversion = t_helpers.form_test_essentials(symbol_, time_interval, is_equity, mic)
            time_series = self.prepare_table_for_case(symbol_, time_interval, is_equity, mic, inserted_rows=25)
            start = time_series[0]
            end = time_series[-1]
            middle = time_series[randint(1, 23)]
            start_fetch = db_functions.fetch_datapoint_by_date(
                start["datetime_object"], symbol_, time_interval, is_equity, mic)
            end_fetch = db_functions.fetch_datapoint_by_date(
                end["datetime_object"], symbol_, time_interval, is_equity, mic)
            middle_fetch = db_functions.fetch_datapoint_by_date(
                middle["datetime_object"], symbol_, time_interval, is_equity, mic)
            for pair in [(start, start_fetch), (end, end_fetch), (middle, middle_fetch)]:
                self.assertCandleMatchesData(pair[1], pair[0], time_conversion, is_equity)

    def test_fetch_generic_datapoint_by_ID(self):
        """test fetching a single point of data using primary key of the table in question"""
        for symbol_, time_interval, mic, is_equity in self.time_table_cases:
            schema_name, table_name, time_conversion = t_helpers.form_test_essentials(
                symbol_, time_interval, mic, is_equity)
            random_table_length = randint(25, 75)
            random_point = randint(1, random_table_length-3)
            time_series = self.prepare_table_for_case(
                symbol_, time_interval, is_equity, mic, inserted_rows=random_table_length)
            start = time_series[0]
            end = time_series[-1]
            middle = time_series[random_point]

            # all the following fetch responses come in a list of only 1 element...
            start_fetch = db_functions.fetch_generic_by_ID_(0, table_name, schema_name)
            end_fetch = db_functions.fetch_generic_by_ID_(
                random_table_length-1, table_name, schema_name)
            middle_fetch = db_functions.fetch_generic_by_ID_(random_point, table_name, schema_name)
            subcases = [
                (0, start, start_fetch),
                (random_table_length - 1, end, end_fetch),
                (random_point, middle, middle_fetch)
            ]
            for sub_case in subcases:
                self.assertEqual(sub_case[0], sub_case[2][0])  # "ID" equivalence
                self.assertCandleMatchesData(
                    fetched_candle=sub_case[2], data_candle=sub_case[1],
                    time_string_conversion=time_conversion, is_equity=is_equity
                )

    def test_fetch_generic_data_by_IDs(self):
        with self.assertRaises(UndefinedTable):
            db_functions.fetch_generic_range_by_IDs('publi', ''.join(choices('awgv', k=7)))

        # test for some public tables
        for t in self.public_tables:
            last_id_possible = db_functions.fetch_generic_last_ID(*t)
            sub_cases = [
                (None, None),
                (0, last_id_possible),
                (randint(-46943, -24), last_id_possible + randint(20, 49237)),
                (None, last_id_possible + randint(20, 49237)),
                (randint(-46943, -24), None),
            ]
            # some default as well as edge cases
            for sub_case in sub_cases:
                self.assertDatabaseAssumedRowsNumber(
                    *t, data_length=len(db_functions.fetch_generic_range_by_IDs(*t, *sub_case)))
            # since the check inside is ">=" and "<=" -> a single row of exactly the same id
            # should be possible to obtain when start and end are equal
            self.assertEqual(1, len(db_functions.fetch_generic_range_by_IDs(*t, start_id=0, end_id=0)))
            random_row = randint(1, last_id_possible)
            self.assertEqual(1, len(db_functions.fetch_generic_range_by_IDs(*t, start_id=random_row, end_id=random_row)))
            # middle of the data
            st_row = randint(0, last_id_possible - 1)
            e_row = randint(st_row, last_id_possible)
            self.assertEqual(
                e_row - st_row + 1, len(db_functions.fetch_generic_range_by_IDs(*t, start_id=st_row, end_id=e_row)))

        # cases for artificial timetables
        for case in self.time_table_cases:
            symbol_, time_interval, mic, is_equity = case
            schema_name, table_name, time_conversion = t_helpers.form_test_essentials(
                symbol_, time_interval, mic, is_equity)
            random_table_length = randint(25, 70)
            _time_series = self.prepare_table_for_case(
                symbol_, time_interval, is_equity, mic, inserted_rows=random_table_length)
            t = schema_name, table_name
            last_id_possible = db_functions.fetch_generic_last_ID(*t)
            sub_cases = [
                (None, None),
                (0, last_id_possible),
                (randint(-46943, -24), last_id_possible + randint(20, 49237)),
                (None, last_id_possible + randint(20, 49237)),
                (randint(-46943, -24), None),
            ]
            # some default as well as edge cases
            for sub_case in sub_cases:
                self.assertDatabaseAssumedRowsNumber(
                    *t, data_length=len(db_functions.fetch_generic_range_by_IDs(*t, *sub_case)))
            # since the check inside is ">=" and "<=" -> a single row of exactly the same id
            # should be possible to obtain when start and end are equal
            self.assertEqual(1, len(db_functions.fetch_generic_range_by_IDs(*t, start_id=0, end_id=0)))
            random_row = randint(1, last_id_possible)
            self.assertEqual(1, len(db_functions.fetch_generic_range_by_IDs(*t, start_id=random_row, end_id=random_row)))
            # middle of the data
            st_row = randint(0, last_id_possible - 1)
            e_row = randint(st_row, last_id_possible)
            fetched_timeseries = db_functions.fetch_generic_range_by_IDs(*t, start_id=st_row, end_id=e_row)
            self.assertEqual(e_row - st_row + 1, len(fetched_timeseries))
            data_to_check = _time_series[st_row:e_row] + [_time_series[e_row]]
            for fetched_point, data_point in zip(fetched_timeseries, data_to_check):
                self.assertCandleMatchesData(fetched_point, data_point, time_conversion, is_equity)

    def test_fetch_data_by_timestamps(self):
        """test fetching subset of time series using dates and commonly used identifiers (like symbol/mic... etc.)"""
        # function signature:
        # db_functions.fetch_data_by_dates(
        #     symbol: str, time_interval: str, is_equity: bool, mic_code: str,
        #     start_date: datetime, end_date: datetime, time_span: timedelta, trading_time_span: int)
        mock_date = datetime(year=randint(2000, 2010), month=randint(1, 12), day=randint(1, 20))
        exception_cases = [
            # too few date arguments
            ("AAPL", "1min", True, "XNGS", None, None, None, None, LookupError),
            ("AAPL", "1min", True, "XNGS", mock_date, None, None, None, LookupError),
            ("AAPL", "1min", True, "XNGS", None, mock_date, None, None, LookupError),
            ("AAPL", "1min", True, "XNGS", None, None, timedelta(days=randint(1, 49)), None, LookupError),
            ("AAPL", "1min", True, "XNGS", None, None, None, randint(1, 49), LookupError),

            # too few arguments but also wrong table resolve data:
            # wrong date combination errors tripping first rule
            ("AAAPL", "1min", True, "XNGS", None, None, None, randint(1, 49), LookupError),
            ("AAAPL", "1min", None, "XNGS", None, None, None, randint(1, 49), LookupError),
            ("AAAPL", "1day", None, "XNGS", None, None, None, randint(1, 49), LookupError),
            ("AAAPL", "1day", None, None, None, None, None, randint(1, 49), LookupError),
            ("USD/GBP", "7apj", None, None, None, None, None, randint(1, 49), LookupError),  # -> time sanitizer
            ("USD/GBP", "1day", None, "XNGS", None, None, None, randint(1, 49), LookupError),
            ("USD/GBP", "1day", True, "XNGS", None, None, None, randint(1, 49), LookupError),

            # bad symbols, that don't resolve inside the schema/table check
            ("AAAAPL", "1min", None, None,
             mock_date, None, None, randint(1, 49),  db_functions.DataUncertainError),
            ("USD/CAD", "1min", None, "XNAADOAWI",
             mock_date, None, None, randint(1, 49),  db_functions.DataUncertainError),

            # time sanitizer tripping before everything further
            # it should actually be distinguished, probably by intercepting a error message that should
            # include some text hinting at this part triggering, but let's leave it for a moment
            ("USD/GBP", "7apj", False, None, mock_date, None, None, randint(1, 49), ValueError),
            ("USD/GBP", "7apj", True, "XNGS", mock_date, None, None, randint(1, 49), ValueError),
            ("AAPL", "7apj", True, "XNGS", mock_date, None, None, randint(1, 49), ValueError),

            # next, Value errors should get triggered (and other affiliated), while checking table-resolving
            # internal checks for absence/presence of MIC in the identifier
            ("USD/GBP", "1min", None, "XNGS", mock_date, None, None, randint(1, 49), ValueError),
            ("AAPL", "1min", None, None, mock_date, None, None, randint(1, 49), ValueError),
            ("AAPL", "1min", True, None, mock_date, None, None, randint(1, 49), ValueError),
            # # these particular is wrong in other way (stock as f_pair), but we check internal MIC identifier still
            ("AAPL", "1min", False, "XNGS", mock_date, None, None, randint(1, 49), ValueError),
            ("AAPL", "1min", False, "XANSAFD", mock_date, None, None, randint(1, 49), ValueError),
            ("AAPL", "1min", False, "XANSAFD", mock_date, None, None, randint(1, 49), ValueError),

            # various combinations of improper data -> error resolving table locations (proper date input structure)
            ("AAAPL", "1min", None, "XNGS",
             mock_date, None, None, randint(1, 49), db_functions.DataUncertainError),
            ("AAPL", "1min", True, "XNAAGS",
             mock_date, None, None, randint(1, 49), db_functions.TimeSeriesNotFoundError),
            ("AAAPL", "1day", None, None,
             mock_date, None, None, randint(1, 49), db_functions.DataUncertainError),
            # # bad symbol -> when forced (lack of actual table) and when not forced, lack of db data
            ("AAAPL", "1day", True, "XNGS",
             mock_date, None, None, randint(1, 49), db_functions.TimeSeriesNotFoundError),
            ("AAAPL", "1min", None, None,
             mock_date, None, None, randint(1, 49), db_functions.DataUncertainError),
            ("USD/GBP", "1min", False, None,
             mock_date, None, None, randint(1, 49), db_functions.TimeSeriesNotFoundError),
        ]
        for case in exception_cases:
            symbol, time_interval, is_equity, mic_code = case[0:4]
            start_date, end_date, time_span, trading_time_span = case[4:-1]
            error_raised = case[-1]
            with self.assertRaises(error_raised):
                print(case)
                db_functions.fetch_data_by_dates(
                    symbol, time_interval, is_equity, mic_code,
                    start_date, end_date, time_span, trading_time_span
                )

        for table_case in self.time_table_cases:
            symbol_, time_interval, mic, is_equity = table_case
            schema_name, table_name, time_conversion = t_helpers.form_test_essentials(
                symbol_, time_interval, mic, is_equity)
            inserted_data = self.prepare_table_for_case(
                symbol_, time_interval, is_equity, mic, inserted_rows=25)

            # below is pretty much 1-to-1 copy of cases from "calculate fetch bracket"
            sub_cases = []
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'],
                ending_timestamp=inserted_data[-1]['datetime_object']
            ))
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(30, 90)),
                ending_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(30, 90))
            ))
            sub_cases.extend(t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[randint(3, 10)]['datetime_object'],
                ending_timestamp=inserted_data[randint(20, 24)]['datetime_object']
            ))
            c = t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(60, 70)),
                ending_timestamp=inserted_data[0]['datetime_object'] - timedelta(days=randint(10, 15)),
                raised_exception=db_functions.DataNotPresentError,
            )
            c.pop(3)
            sub_cases.extend(c)
            c = t_helpers.time_bracket_case_generator(
                reference_dataset=inserted_data,
                starting_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(10, 15)),
                ending_timestamp=inserted_data[-1]['datetime_object'] + timedelta(days=randint(60, 70)),
                raised_exception=db_functions.DataNotPresentError,
            )
            c.pop()
            sub_cases.extend(c)
            if "day" in time_interval:
                prior_weekday = next((
                    day for day in inserted_data if day['datetime_object'].isoweekday() == 5
                ))['datetime_object']
                prior_weekday += timedelta(days=1)
                next_weekday = prior_weekday + timedelta(days=7 + randint(1, 2))
                sub_cases.extend(t_helpers.time_bracket_case_generator(
                    reference_dataset=inserted_data,
                    starting_timestamp=prior_weekday,
                    ending_timestamp=next_weekday,
                ))

            # these cases should in actuality reflect the timestamp fetch results and not row ID
            # checks, unlike previous methods (calculate_fetch_bracket namely)
            sub_cases.extend([
                (inserted_data[0]['datetime_object'], None, None,
                 len(inserted_data) * 3, (0, len(inserted_data)-1), None),
                (inserted_data[0]['datetime_object'], None, timedelta(days=randint(300, 500)),
                 None, (0, len(inserted_data) - 1), None),
                (None, inserted_data[-1]['datetime_object'], timedelta(days=randint(300, 500)),
                 None, (0, len(inserted_data) - 1), None),
                (None, inserted_data[-1]['datetime_object'], None,
                 len(inserted_data) * 3, (0, len(inserted_data) - 1), None),
            ])

            for id_, sub_case in enumerate(sub_cases):
                start_date, end_date, time_span, trading_time_span, predicted_answer, raised_exception = sub_case
                message = f'test case: {id_=}, main:{table_case} sub: {sub_case}'
                print(message)
                if raised_exception:
                    # we actually don't need to raise exception, just return
                    # empty list, that's far more forgiving and raising would have been unnecessarily complicated
                    data_fetched = db_functions.fetch_data_by_dates(
                            symbol_, time_interval, is_equity, mic_code=mic, start_date=start_date,
                            end_date=end_date, time_span=time_span, trading_time_span=trading_time_span)
                    self.assertEqual(data_fetched, [])
                    continue
                a, b = predicted_answer
                inserted_answer_data = inserted_data[a:b] + [inserted_data[b]]
                data_fetched = db_functions.fetch_data_by_dates(
                    symbol_, time_interval, is_equity, mic_code=mic, start_date=start_date,
                    end_date=end_date, time_span=time_span, trading_time_span=trading_time_span)
                for fetched_candle, inserted_candle in zip(data_fetched, inserted_answer_data):
                    self.assertCandleMatchesData(fetched_candle, inserted_candle, time_conversion, is_equity)


if __name__ == '__main__':
    unittest.main()
