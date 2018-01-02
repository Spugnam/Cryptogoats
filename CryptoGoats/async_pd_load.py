
import os
import sys
import json
import time
import asyncio
from os import _exit
import ccxt
import psycopg2
import pandas as pd


########################################################################
# Asynchronous database load
########################################################################

# create Pandas df
pd.set_option('display.float_format', lambda x: '%.10f' % x) # display 10 digits
prices = pd.DataFrame(columns=['timestamp', 'exchange', 'pair',
                               'exchange_timestamp', 'bids',
                               'high_bid', 'asks', 'low_ask', 'volume'])
# convert columns to float
prices[['high_bid','low_ask']] = prices[['high_bid','low_ask']].apply(pd.to_numeric)

# Create and connect to all configured exchanges

with open('./PostgreSQL/config_exchanges.json') as f:
    config = json.load(f)

########################################################################

async def load_order_book(exchange, pair):
    """
    add order_book to Pandas dataframe
    pair = 'ETH/BTC'
    TODO: move to helpers file
    """
    orderbook = exchange.fetch_order_book(pair)

    row = [time.time(), exchange.id, pair, orderbook['timestamp'],\
     ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['bids'][:5])),\
     orderbook['bids'][0][0], # highest bid
    ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['asks'][:5])),\
    orderbook['asks'][0][0], # lowest ask
    1000]

    # print(row)
    return(row)

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
    global prices
    for pair in arbitrableSymbols:
        for id in exchangesBySymbol[pair]:
            row = await load_order_book(exchanges[id], pair)
            prices = prices.append(pd.DataFrame([row], columns=prices.columns)\
                                , ignore_index=True)
        # print biggest spread for pair
        bb_exchange, ba_exchange, spread = get_spread(pair)
        print("Biggest % spread for ", pair, spread,
              "sell@", bb_exchange, ", buy@", ba_exchange)

########################################################################
# spread

def get_spread(pair, df=prices):
    best_bid = prices.loc[prices['pair'] == pair, ['high_bid']].unstack().max()
    bb_idx = prices.loc[prices['pair'] == pair, ['high_bid']].idxmax()
    bb_exchange = prices.loc[bb_idx, 'exchange'].values[0]

    best_ask = prices.loc[prices['pair'] == pair, ['low_ask']].unstack().min()
    ba_idx = prices.loc[prices['pair'] == pair, ['low_ask']].idxmin()
    ba_exchange = prices.loc[ba_idx, 'exchange'].values[0]

    spread = 100 * (best_bid - best_ask) / best_bid

    return(bb_exchange, ba_exchange, spread)

########################################################################
# Download dataframe to psql

# # Connect to PostgreSQL database
# with open('./PostgreSQL/config_psql.json') as f:
#     config = json.load(f)
#
# conn = psycopg2.connect(**config)

########################################################################
# Load prices infinite loop

# prices
prices[prices['pair']=='NEO/ETH']

@asyncio.coroutine
def load_prices():
    starttime = time.time()
    counter = 1
    while True and counter < 5:
        asyncio.ensure_future(load_arbitrableSymbols())
        counter += 1
        yield from asyncio.sleep(120.0 - ((time.time() - starttime) % 120.0))

task = asyncio.Task(load_prices())
loop = asyncio.get_event_loop()
loop.run_until_complete(task)
