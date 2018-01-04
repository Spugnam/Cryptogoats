
import os
import sys
import json
import time
import threading
from os import _exit
import ccxt
import psycopg2


########################################################################
# Asynchronous database load
########################################################################


# Connect to PostgreSQL database
with open('./PostgreSQL/config_psql.json') as f:
    config = json.load(f)

conn = psycopg2.connect(**config)

# Create and connect to all configured exchanges

with open('./PostgreSQL/config_exchanges.json') as f:
    config = json.load(f)


exchanges = {}
for id in ccxt.exchanges:  # list of exchanges id ['acx', bittrex'...]
    if id in config:
        exchange = getattr(ccxt, id) # exchange becomes function bittrex()
        exchanges[id] = exchange(config[id])


# Schedule job with threading
import time
import threading

def insert_order_book(exchange, pair, conn):
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

############################################
# loop to load prices every minute

counter = 1
starttime=time.time()
while True:
    # first reload all markets
    for id, exchange in exchanges.iteritems():
        _ = exchange.load_markets(reload=True)
    # store order books
    for id, exchange in exchanges.iteritems():
        for pair in exchange.symbols:   # for pair, _ in exchange.load_markets().iteritems():
            thread = threading.Thread(target=insert_order_book,
                                      args=(exchange, pair, conn))
            # thread.daemon = True
            thread.start()
    print("Finished round ", counter)
    counter += 1
    time.sleep(60.0 - ((time.time() - starttime) % 60.0))




# print pair for all exchanges
for idb, exchange in exchanges.items():
    try:
        orderbook = exchange.fetch_order_book('ETH/BTC')
        print(orderbook['asks'][0][0])
    except:
        pass

ccxt.exchanges[9]
exchanges
