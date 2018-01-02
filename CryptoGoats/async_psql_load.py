import os
import sys
import json
import time
import asyncio
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

########################################################################

async def insert_order_book(exchange, pair, conn):
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

# Example exchange initilization
# exchange = ccxt.bittrex(config['bittrex'])

exchanges = {}
for id in ccxt.exchanges:  # list of exchanges id ['acx', bittrex'...]
    if id in config:
        exchange = getattr(ccxt, id) # exchange becomes function bittrex()
        exchanges[id] = exchange(config[id])

# Load all markets
for id, exchange in exchanges.items(): # Py3: items() instead of iteritems()
    _ = exchange.load_markets(reload=True)

# Find arbitrable paris (in more than 1 exchange)
allSymbols = [symbol for _, exchange in exchanges.items() for symbol in exchange.symbols]
uniqueSymbols = list(set(allSymbols))
# filter out symbols that are not present on at least two exchanges
arbitrableSymbols = sorted([symbol for symbol in uniqueSymbols if allSymbols.count(symbol) > 1])

# TODO assert arbitrableSymbols not null
# arbitrableSymbols

from collections import defaultdict
exchangesBySymbol = defaultdict()

for pair in arbitrableSymbols:
    for id, exchange in exchanges.items():
        if pair in exchange.symbols:
            try:
                exchangesBySymbol[pair].append(id)
            except KeyError:
                exchangesBySymbol[pair] = [id]

async def load_arbitrableSymbols():
    for pair in arbitrableSymbols:
        for id in exchangesBySymbol[pair]:
            asyncio.ensure_future(insert_order_book(exchanges[id], pair, conn))

# Test
# exchangesBySymbol

##############
@asyncio.coroutine
def periodic():
    starttime = time.time()
    while True:
        asyncio.ensure_future(load_arbitrableSymbols())
        yield from asyncio.sleep(60.0 - ((time.time() - starttime) % 60.0))

task = asyncio.Task(periodic())
loop = asyncio.get_event_loop()
loop.run_until_complete(task)
