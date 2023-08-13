from ast import literal_eval
import psycopg2
from db_functions.db_helpers import connection_dict, db_string_converter


# create queries
query_create_table_for_index = """

"""
# insert queries
query_insert_index_data = """
INSERT INTO {time_series_schema}.\"{equity_symbol}_{market_identification_code}\" 
(\"ID\", datetime, open, close, high, low, volume) 
VALUES (
    {index},
    {timestamp}:: timestamp without time zone,
    {open_price},
    {close_price},
    {high_price},
    {low_price},
    {volume}
);
"""


def insert_equity_historical_data(historical_data: list[dict], equity_symbol: str, mic_code: str, time_interval: str):
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        #  iterate from oldest to newest - new rows will be appended to the farthest row anyway
        for rownum, candle in enumerate(historical_data[::-1]):
            query_dict = {
                "index": rownum,
                "equity_symbol": equity_symbol,
                "time_series_schema": f'"{time_interval}_time_series"',
                "market_identification_code": mic_code,
                "timestamp": f"'{candle['datetime']}'",
                "open_price": candle['open'],
                "close_price": candle['close'],
                "high_price": candle['high'],
                "low_price": candle['low'],
                "volume": candle['volume'],
            }
            cur.execute(query_insert_index_data.format(**query_dict))
            conn.commit()
        cur.close()


if __name__ == '__main__':
    data = []
    with open("otex_series_OG.txt", 'r', newline='\n', encoding='UTF-8') as time_series_file:
        for row in time_series_file:
            data_point = literal_eval(row)
            data.append(data_point)
    # print(data[0], type(data_point), type(data[0]))

    insert_equity_historical_data(data, equity_symbol='OTEX', mic_code="XNGS", time_interval="1min")
