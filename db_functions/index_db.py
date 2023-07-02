from ast import literal_eval
import psycopg2
from db_helpers import connection_dict, db_string_converter


# create queries
query_create_table_for_index = """

"""
# insert queries
query_insert_index_data = """
INSERT INTO time_series.\"{equity_symbol}_{exchange_symbol}\" (\"ID\", datetime, open, close, high, low, volume) 
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


def insert_index_historical_data(historical_data: list[dict], index_symbol: str, exchange_symbol: str):
    with psycopg2.connect(**connection_dict) as conn:
        cur = conn.cursor()
        #  iterate from oldest to newest - new rows will be appended to the farthest row anyway
        for rownum, candle in enumerate(historical_data[::-1]):
            query_dict = {
                "index": rownum,
                "equity_symbol": index_symbol,
                "exchange_symbol": exchange_symbol,
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
    with open("otex_series.txt", 'r', newline='\n', encoding='UTF-8') as time_series_file:
        for row in time_series_file:
            data_point = literal_eval(row)
            data.append(data_point)
    # print(data[0], type(data_point), type(data[0]))

    insert_index_historical_data(data, index_symbol='OTEX', exchange_symbol="NASDAQ")
