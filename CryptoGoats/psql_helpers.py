import asyncio
import ccxt


async def psql_insert_order_book(exchange, pair, conn):
    """
    Insert record into psql
    pair = 'ETH/BTC'
    TODO: move to helpers file
    """
    cur = conn.cursor()
    orderbook = exchange.fetch_order_book(pair)
    pair_base = pair.split('/')[0]
    pair_quote = pair.split('/')[1]

    try:
        cur.execute(
            """INSERT INTO prices (timestamp, exchange, pair_base, pair_quote, exchange_timestamp, bids, asks, volume)
            VALUES(current_timestamp, %s, %s, %s, current_timestamp, %s, %s, %s)""",
            (exchange.id, pair_base, pair_quote,
             ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['bids'][:5])), # format bids into string
             ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['asks'][:5])),
             10000))
        conn.commit()
        print("Completed insertion for", pair, exchange.id)
    except Exception as error:
        print(error)
        conn.rollback()
