from datetime import datetime
import unittest

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
    def assertSchemaExist(schema_name):
        with psycopg2.connect(**helpers._connection_dict) as conn:
            cur = conn.cursor()
            cur.execute(helpers._information_schema_check)
            result = cur.fetchall()
            assert schema_name in [r[0] for r in result], f"schema {schema_name} not in database"

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
        full_procedures.time_series_save(**params)
        last_datapoint: datetime = db_functions.time_series_latest_timestamp(
            symbol=params['symbol'], time_interval=params['time_interval'], is_equity=params['is_equity'],
            mic_code=params['market_identification_code']
        )
        self.assertIsNotNone(last_datapoint)
        # we check condition only up to a certain day
        today = datetime.today()
        if not is_workingday(today):
            days_to_subtract = today.isoweekday() - 5
            last_allowed_day = datetime.fromtimestamp(today.timestamp() - days_to_subtract * 24 * 60 * 60)
        else:
            last_allowed_day = today
        self.assertEqual(last_datapoint.year, last_allowed_day.year)
        self.assertEqual(last_datapoint.month, last_allowed_day.month)
        self.assertEqual(last_datapoint.day, last_allowed_day.day)

    def test_update_ticker(self):
        """first download and save the ticker partially, then update it up to the most recent date (possibly today)"""
        # we use daily data here as well, to save on the number of queries
        params = {
            "symbol": "NVDA",
            "market_identification_code": "XNGS",
            "time_interval": "1day",
            "verbose": True,
            "key_switcher": ProcedureTests.key_switcher,
            "is_equity": True
        }

