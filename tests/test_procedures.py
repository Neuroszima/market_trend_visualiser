from datetime import datetime
import unittest
from random import randint

import psycopg2
import db_functions, api_functions
import db_functions.db_helpers as helpers

import test_db

import full_procedures


def is_workingday(date: datetime):
    return 1 <= date.isoweekday() <= 5


class ProcedureTests(unittest.TestCase):
    key_switcher = api_functions.api_key_switcher()  # all available keys used for that test suite

    def setUp(self) -> None:
        db_functions.purge_db_structure()
        self.t_db = test_db.DBTests()

    def assertTableExist(self, table_name, schema_name):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._information_schema_table_check.format(
                table_name=db_functions.db_string_converter(table_name),
                schema=db_functions.db_string_converter(schema_name),
            ))
            result = cur.fetchall()
            self.assertTrue(result)

    @staticmethod
    def assertSchemaExist(schema_name: str):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._information_schema_check)
            result = cur.fetchall()
            assert schema_name in [r[0] for r in result], f"schema {schema_name} not in database"

    def assertTableHasRows(self, schema_name: str, table_name: str, correct_num_of_rows: int):
        """check if certain table within schema has as many rows as needed"""
        err_msg = f"there are %s rows in {schema_name}.{table_name} and there should be {correct_num_of_rows}"
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._table_rows_quantity.format(schema=schema_name, table_name=table_name))
            result = cur.fetchall()
            self.assertEqual(result[0][0], correct_num_of_rows, msg=err_msg % (str(result[0][0])))

    def assertCompareRowCount(self, schema_name: str, table_name: str, compared_num_of_rows: int, compare_method: str):
        """
        uses magic methods to check a specific number against the number of rows that
        table has at the moment of check

        magic methods come from "compared_num_of_rows" `int` object, so the ``self`` in that method is cmp_num_of_rows,
        while the ``other`` is the num of rows from the table. In other words, if we want to check if we have
        more than 70 rows in a table -> we should check if "70 is less than" number of rows in a table using
        this method

        a bit more powerful than previous assertTableHasRows (can be subset as "eq" check)
        :param schema_name: schema where table is located
        :param table_name: full name of table
        :param compare_method: name of magic method that should do the check
        (e.g. "eq", "lt" -> without "__" surrounding the name)
        :param compared_num_of_rows: the total count of rows a table should or shouldn't have
        """
        err_message = f"counts do not match the studied case: {schema_name}.{table_name}, " + \
                      f"{compared_num_of_rows} should be {compare_method} %s"
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._table_rows_quantity.format(schema=schema_name, table_name=table_name))
            result = cur.fetchall()
            row_cmp_method = getattr(compared_num_of_rows, f"__{compare_method}__")
            self.assertTrue(row_cmp_method(result[0][0]), msg=err_message % (str(result[0][0])))

    def assertTimeSeriesLatestDate(
            self, symbol: str, time_interval: str, is_equity: bool, mic_code: str, end_date: datetime | None = None):
        """
        checks if the latest datapoint is of the right date/timestamp, like the one passed as an argument

        this check is simplified and only accounts for accuracy up to a day (not hour/minute/second),
        Saturdays and Sundays are taken into account, but not market/bank holidays (refer to 19th Feb 2024 as example)
        """
        err_msg = "latest timestamp does not fit in the allowed date bracket [%s, %s, %s]"
        last_datapoint_timestamp = db_functions.time_series_latest_timestamp(
            symbol, time_interval, is_equity, mic_code
        )
        self.assertIsNotNone(last_datapoint_timestamp)
        # we check condition only up to a certain day - skip saturday and sunday (market not open at these days)
        today = datetime.utcnow()
        if end_date:
            last_allowed_day = end_date
        else:
            last_allowed_day = today
        last_allowed_day = datetime.fromtimestamp(last_allowed_day.timestamp() - 3 * 24 * 60 * 60)
        assert last_allowed_day < last_datapoint_timestamp < today, \
            err_msg % (last_allowed_day, last_datapoint_timestamp, today)

    def test_setup_database(self):
        """test destructive database rebuild - check if all the elements are in place"""
        full_procedures.rebuild_database_destructively()
        tables_in_database_with_schemas = [
            ('currencies', 'public'),
            ('markets', 'public'),
            ('investment_types', 'public'),
            ('forex_currency_groups', 'public'),
            ('timezones', 'public'),
            ('stocks', 'public'),
            ('time_tracking_info', 'public'),
            ('forex_pairs', 'public'),
            ('plans', 'public'),
            ('countries', 'public'),
            ('tracked_indexes', 'public')  # this is technically a view
        ]
        functions_in_database = [
            ('generate_financial_view_1min', 'public'),
            ('generate_financial_view_1day', 'public'),
            ('generate_forex_view', 'public'),
            ('check_is_stock', 'public'),
        ]
        additional_schemas = ["1min_time_series", "1day_time_series", "forex_time_series"]
        for table, schema in tables_in_database_with_schemas:
            self.assertTableExist(table, schema)
        for schema in additional_schemas:
            self.assertSchemaExist(schema)
        function_list = [r[1:] for r in helpers.list_nonstandard_functions_()]
        for function_name, schema in functions_in_database:
            self.assertIn(
                (function_name, schema, 'FUNCTION'), function_list,
                msg=f"function not found {function_name}"
            )

    def test_fill_database(self):
        """test filling database with data related to market tickers and miscellaneous information"""
        full_procedures.rebuild_database_destructively()
        full_procedures.fill_database(ProcedureTests.key_switcher)
        # as assertion method describes, we perform checks from the perspective of the checked number,
        # not number of rows. I.e. "50 is less than number of rows in a table XYZ"
        content_counts_tests = [
            ("public", "countries", 50, "lt"),
            ("public", "plans", 4, "eq"),
            ("public", "currencies", 40, "lt"),
            ("public", "timezones", 45, "lt"),
            ("public", "investment_types", 16, "eq"),
            ("public", "forex_pairs", 1200, "lt"),
            ("public", "stocks", 90000, "lt"),
            ("public", "markets", 70, "lt"),
            ("public", "forex_currency_groups", 4, "eq"),
        ]
        for schema_name, table_name, predicted_rowcounts, compare_method in content_counts_tests:
            self.assertCompareRowCount(schema_name, table_name, predicted_rowcounts, compare_method)

    def test_save_ticker_fully(self):
        """this checks a whole download process so it needs a lot of tokens"""
        # add a couple of definitions to the database - XAU/USD as forex pair, AAPL as equity
        full_procedures.rebuild_database_destructively()
        self.t_db.save_forex_sample()
        self.t_db.save_markets()
        self.t_db.save_equities()

        # real tests
        test_cases = [
            ("AAPL", "XNGS", "1day", True),
            ("XAU/USD", None, "1day", False),
            ("AAPL", "XNGS", "1min", True),
            ("XAU/USD", None, "1min", False),
        ]

        for symbol_, mic, timeframe, is_equity in test_cases:
            params = {
                "symbol": symbol_,
                "market_identification_code": mic,
                "time_interval": timeframe,
                "key_switcher": ProcedureTests.key_switcher,
                # "verbose": True,
            }
            timestamp_test_params = {
                "symbol": symbol_,
                "mic_code": mic,
                "time_interval": timeframe,
                "is_equity": is_equity
            }
            full_procedures.time_series_save(**params)
            last_datapoint: datetime = db_functions.time_series_latest_timestamp(**timestamp_test_params)
            self.assertIsNotNone(last_datapoint)
            self.assertTimeSeriesLatestDate(**timestamp_test_params)

    def test_update_ticker(self):
        """first download and save the ticker partially, then update it up to the most recent date (possibly today)"""
        full_procedures.rebuild_database_destructively()
        # using predetermined data to fill in the database with couple necessary rows
        # following saves XAU/USD pair, so it is automatically checked if it detects it as ticker or not
        self.t_db.save_forex_sample()
        self.t_db.save_markets()
        self.t_db.save_equities()
        test_cases = [
            ("AAPL", "XNGS", "1day", True),  # v
            ("XAU/USD", None, "1day", False),  # v
            ("AAPL", "XNGS", "1min", True),  # v
            ("XAU/USD", None, "1min", False),  # v
        ]

        for symbol_, mic, timeframe, is_equity in test_cases:
            update_params = {
                "symbol": symbol_,
                "market_identification_code": mic,
                "time_interval": timeframe,
                # "verbose": True,
                "key_switcher": ProcedureTests.key_switcher,
            }
            query_params = {
                "symbol": symbol_,
                "mic_code": mic,
                "time_interval": timeframe,
            }
            with self.assertRaises(db_functions.TimeSeriesNotFoundError):  # table does not exist
                full_procedures.time_series_update(**update_params)

            # following prepares a database for updating, with additional helper assertion checks

            # time up to which the data will be downloaded and from which function has to pick up and update
            end_initial_batch = datetime(year=2022, month=7, day=13)
            # following limits the need for the data to download which shortens test duration and saves tokens
            start_date = datetime(year=2021, month=7, day=20) if "min" in timeframe else None
            data = api_functions.download_market_ticker_history(
                **query_params, key_switcher=ProcedureTests.key_switcher,
                start_date=start_date, end_date=end_initial_batch
            )
            query_params["is_equity"] = is_equity  # after this, we need to include this parameter for other functions
            db_functions.create_time_series(**query_params)
            db_functions.insert_equity_historical_data(historical_data=data, **query_params)
            schema_name = f"{timeframe}_time_series" if is_equity else "forex_time_series"
            table_name = f"{symbol_}_{mic}" if is_equity else "%s_%s_%s" % (*symbol_.split("/"), timeframe)
            self.assertTableHasRows(schema_name, table_name, len(data))
            self.assertTimeSeriesLatestDate(**query_params, end_date=end_initial_batch)

            # proper update tests
            with self.assertRaises(db_functions.TimeSeriesExists):  # date in DB already covered error
                full_procedures.time_series_update(**update_params, end_date=datetime(
                    year=randint(2006, 2011), month=randint(1, 12), day=randint(1, 27)
                ))
            full_procedures.time_series_update(**update_params)
            self.assertTimeSeriesLatestDate(**query_params)


if __name__ == '__main__':
    unittest.main()
