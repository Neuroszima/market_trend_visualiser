from datetime import datetime
import unittest
from random import randint

import psycopg2
import db_functions, api_functions
import db_functions.db_helpers as helpers

import full_procedures


def is_workingday(date: datetime):
    return 1 <= date.isoweekday() <= 5


class ProcedureTests(unittest.TestCase):
    key_switcher = api_functions.api_key_switcher()  # all available keys used for that test suite

    def setUp(self) -> None:
        db_functions.purge_db_structure()

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

    def assertDatabaseHasRows(self, schema_name: str, table_name: str, correct_num_of_rows: int):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._table_rows_quantity.format(schema=schema_name, table_name=table_name))
            result = cur.fetchall()
            self.assertEqual(result[0][0], correct_num_of_rows)

    def assertTimeSeriesLatestDate(
            self, symbol: str, time_interval: str, is_equity: bool, mic_code: str, end_date: datetime | None = None):
        """
        checks if the latest datapoint is of the right date/timestamp, according to the one passed as an argument

        this check is simplified and only accounts for accuracy up to a day (not hour/minute/second),
        Saturdays and Sundays are taken into account, but not market/bank holidays (refer to 19th Feb 2024 as example)
        """
        last_datapoint_timestamp = db_functions.time_series_latest_timestamp(
            symbol, time_interval, is_equity, mic_code
        )
        self.assertIsNotNone(last_datapoint_timestamp)
        # we check condition only up to a certain day - skip saturday and sunday (market not open at these days)
        if end_date:
            last_allowed_day = end_date
        else:
            last_allowed_day = datetime.today()
        if not is_workingday(last_allowed_day):
            days_to_subtract = last_allowed_day.isoweekday() - 5
            last_allowed_day = datetime.fromtimestamp(last_allowed_day.timestamp() - days_to_subtract * 24 * 60 * 60)
        self.assertEqual(last_datapoint_timestamp.year, last_allowed_day.year)
        self.assertEqual(last_datapoint_timestamp.month, last_allowed_day.month)
        self.assertEqual(last_datapoint_timestamp.day, last_allowed_day.day)

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
        additional_schemas = ["1min_time_series", "1day_time_series", "forex_time_series"]
        for table, schema in tables_in_database_with_schemas:
            self.assertTableExist(table, schema)
        for schema in additional_schemas:
            self.assertSchemaExist(schema)

    def test_save_ticker_fully(self):
        """to not waste that many tokens on testing, this method uses daily interval, as it is fewer downloads"""
        full_procedures.rebuild_database_destructively()
        params = {
            "symbol": "NVDA",
            "market_identification_code": "XNGS",
            "time_interval": "1day",
            "verbose": True,
            "key_switcher": ProcedureTests.key_switcher,
            "is_equity": True
        }
        timestamp_test_params = {
            "symbol": params["symbol"],
            "mic_code": params["market_identification_code"],
            "time_interval": params["time_interval"],
            "is_equity": params["is_equity"]
        }
        full_procedures.time_series_save(**params)
        last_datapoint: datetime = db_functions.time_series_latest_timestamp(**timestamp_test_params)
        self.assertIsNotNone(last_datapoint)
        self.assertTimeSeriesLatestDate(**timestamp_test_params)

    def test_update_ticker(self):
        """first download and save the ticker partially, then update it up to the most recent date (possibly today)"""
        # we use daily data here as well, to save on the number of queries
        full_procedures.rebuild_database_destructively()
        update_params = {
            "symbol": "NVDA",
            "market_identification_code": "XNGS",
            "time_interval": "1day",
            "verbose": True,
            "key_switcher": ProcedureTests.key_switcher,
            "is_equity": True
        }
        query_params = {
            "symbol": "NVDA",
            "mic_code": "XNGS",
            "time_interval": "1day",
        }
        with self.assertRaises(db_functions.TimeSeriesNotFoundError):  # table does not exist
            full_procedures.time_series_update(**update_params)
        end_initial_batch = datetime(year=randint(2022, 2023), month=randint(1, 9), day=randint(1, 26))
        data = api_functions.download_market_ticker_history(
            **query_params, end_date=end_initial_batch, key_switcher=ProcedureTests.key_switcher,
        )
        db_functions.create_time_series(**query_params)
        db_functions.insert_equity_historical_data(historical_data=data, **query_params)
        self.assertDatabaseHasRows(
            "1day_time_series",
            f'{update_params["symbol"]}_{update_params["market_identification_code"]}',
            len(data)
        )
        self.assertTimeSeriesLatestDate(**query_params, is_equity=True, end_date=end_initial_batch)

        # proper update tests
        with self.assertRaises(db_functions.TimeSeriesExists):  # date in DB already covered error
            full_procedures.time_series_update(**update_params, end_date=datetime(
                year=randint(2006, 2011), month=randint(1, 12), day=randint(1, 27)
            ))
        full_procedures.time_series_update(**update_params)
        self.assertTimeSeriesLatestDate(**query_params, is_equity=True)


if __name__ == '__main__':
    unittest.main()
