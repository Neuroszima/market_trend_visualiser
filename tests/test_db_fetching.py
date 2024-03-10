import unittest
from datetime import datetime, timedelta
from random import choices, randint, random
from typing import Callable

# import psycopg2

import db_functions.db_helpers as helpers
# from db_functions.time_series_db import _drop_time_table, _drop_forex_table
import db_functions
import tests.test_db as test_db
import tests.t_helpers as t_helpers


class DBFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_db = test_db.DBTests()
        db_functions.purge_db_structure()
        db_functions.import_db_structure()
        self.save_samples_for_tests()

    def assertFetchCaseCompliant(
            self, fetch_filter_arguments: list | tuple, fetch_function: Callable, possible_result: list | type,
            list_of_database_contents: list):
        msg = f'failed: {fetch_filter_arguments} {fetch_function}'
        if isinstance(possible_result, type):  # exception
            with self.assertRaises(possible_result, msg=f'raising %s' % msg):
                fetch_function(*fetch_filter_arguments)
            return
        fetch_result = fetch_function(*fetch_filter_arguments)
        self.assertEqual(len(possible_result), len(fetch_result), msg=f"case %s" % msg)
        if possible_result:
            for entry in possible_result:
                self.assertIn(list_of_database_contents[entry], fetch_result)

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
            if time_interval in ['1day']:
                time_conversion = '%Y-%m-%d'
            elif time_interval in ['1min']:
                time_conversion = '%Y-%m-%d %H:%M:%S'
            inserted_data = self.prepare_table_for_case(
                symbol=symbol, time_interval=time_interval,
                mic=mic, is_equity=is_equity
            )
            date_check = datetime.strptime(inserted_data[0]['datetime'], time_conversion)
            point = db_functions.fetch_datapoint_by_date(symbol, time_interval, date_check, is_equity, mic_code=mic)
            self.assertTrue(point)
            incorrect_date = date_check - timedelta(days=randint(1, 45))
            with self.assertRaises(helpers.DataNotPresentError_):
                db_functions.fetch_datapoint_by_date(symbol, time_interval, incorrect_date, is_equity, mic_code=mic)

    def test_locate_closest_datapoint(self):
        """test row locator function that is part of database data fetching functionality"""
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

                fetched_data = db_functions.fetch_datapoint_raw_by_pk(point_id, table_name, schema_name)
                self.assertEqual(point_id, fetched_data[0])
                self.assertEqual(check_date, fetched_data[1])

    def test_calculate_fetch_time_bracket(self):
        """
        test whether the amount (length of data to be fetched)
        and ID's of edges of the range that is about to be queried is ok
        """
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
                    "", "", start_date, end_date, time_span, trading_time_span)

        cases = [
            ("AAPL", "1day", "XNGS", True),
            ("USD/EUR", "1day", None, False),
            ("USD/EUR", "1min", None, False),
            ("AAPL", "1min", "XNGS", True)
        ]
        for case in cases:
            symbol, time_interval, mic, is_equity = case
            schema_name = f"{time_interval}_time_series" if is_equity else "forex_time_series"
            table_name = f"{symbol}_{mic}" if is_equity else "%s_%s_%s" % (*symbol.split("/"), time_interval)
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
                            schema_name, table_name, start_date, end_date, time_span, trading_time_span
                        )
                    continue
                id_bracket_ = db_functions.calculate_fetch_time_bracket(
                    schema_name, table_name, start_date, end_date, time_span, trading_time_span
                )
                self.assertEqual(predicted_answer, id_bracket_, msg=message)

    # TODO - tests for miscellaneous fetching functions here
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
        forex_pairs = [
            (0, "Exotic-Cross", "AED/BRL", "AED", "BRL", "UAE Dirham", "Brazil Real"),
            (1, "Exotic-Cross", "JPY/ZAR", "JPY", "ZAR", "Japanese Yen", "South African Rand"),
            (2, "Exotic", "ARS/USD", "ARS", "USD", "Argentinian Peso", "US Dollar"),
            (3, "Minor", "KGS/RUB", "KGS", "RUB", "Kyrgyzstan som", "Russian Ruble"),
            (4, "Major", "USD/EUR", "USD", "EUR", "US Dollar", "Euro"),
            (5, "Major", "USD/GBP", "USD", "GBP", "US Dollar", "British Pound"),
            (6, "Major", "USD/JPY", "USD", "JPY", "US Dollar", "Japanese Yen"),
        ]
        cases = [
            ('M%', None, None, None, None, None, [3, 4, 5, 6]),
            (None, None, None, None, '%Dollar%', '%Pound%', [5]),
            (None, None, 'USD', None, None, None, [4, 5, 6]),
            (None, 'USD%', None, None, None, None, [4, 5, 6]),
            ('E%', None, None, None, None, None, [0, 1, 2]),
            ('E%', None, None, None, None, None, [0, 1, 2]),
            ("Major", None, None, "GBP", None, None, [5]),
            *[(*[field for field in forex_pairs[index][1:]], [index]) for index in range(len(forex_pairs))]
        ]
        cases.extend([([None if j != i else '' for j in range(6)] + [ValueError]) for i in range(len(forex_pairs[0])-1)])
        for case in cases:
            self.assertFetchCaseCompliant(case[:-1], db_functions.fetch_forex_pairs, case[-1], forex_pairs)

    def test_fetch_plans(self):
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

    @unittest.skip("this is a test stub")
    def test_fetch_markets(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_countries(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_timezones(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_investment_types(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_stocks(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_datapoint_by_date(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_datapoint_raw_by_pk(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_data_from_IDs(self):
        pass

    @unittest.skip("this is a test stub")
    def test_fetch_data_from_timestamps(self):
        pass


