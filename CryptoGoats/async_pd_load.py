
import os
import sys
# root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(root)

import math
import json
import time
import asyncio
from os import _exit
import ccxt.async as ccxt
# TODO fix below
try:
    from CryptoGoats.helpers import *
except:
    pass
try:
    from helpers import *
except:
    pass
import psycopg2
import pandas as pd


################################################################################
# Initialization
################################################################################

# create Pandas df
pd.set_option('display.float_format', lambda x: '%.10f' % x) # display 10 digits
prices = pd.DataFrame(columns=['timestamp', 'exchange', 'pair',
                               'exchange_timestamp',
                               'bids', 'high_bid', 'high_bid_size',
                               'asks', 'low_ask', 'low_ask_size', 'volume'])

# Create and connect to all configured exchanges
with open('./CryptoGoats/config_exchanges.json') as f:
    config = json.load(f)

exchanges = {}
for id in ccxt.exchanges:  # list of exchanges id ['acx', bittrex'...]
    if id in config:
        exchange = getattr(ccxt, id) # exchange becomes function bittrex()
        exchanges[id] = exchange(config[id])

# Load all markets (pairs)
# for id, exchange in exchanges.items(): # Py3: items() instead of iteritems()
#     try:
#         await exchange.load_markets(reload=True)
#     except:
#         pass

for id, exchange in exchanges.items(): # Py3: items() instead of iteritems()
    try:
        _ = asyncio.get_event_loop().run_until_complete(exchange.load_markets(reload=True))
    except:
        pass


# Find arbitrable pairs (in more than 1 exchange)
allSymbols = [symbol for _, exchange in exchanges.items() for symbol in exchange.symbols]
uniqueSymbols = list(set(allSymbols))
# filter out symbols that are not present on at least two exchanges
arbitrableSymbols = sorted([symbol for symbol in uniqueSymbols if allSymbols.count(symbol) > 1])

# Remove FIAT es
# TODO make option in program
FiatCurrencies = ['EUR', 'USD', 'GBP']
arbitrableSymbolsWithoutFiat = []

for symbol in arbitrableSymbols:
    hasFiat = 0
    for fiatcurrency in FiatCurrencies:
        if symbol.find(fiatcurrency) != -1:
            hasFiat = 1
    if hasFiat == 0:
        arbitrableSymbolsWithoutFiat.append(symbol)

arbitrableSymbols = arbitrableSymbolsWithoutFiat

# Create dictionary of exchanges available for each pair
# Used in loop to find arbitrage trades by pair
from collections import defaultdict
exchangesBySymbol = defaultdict()

for pair in arbitrableSymbols:
    for id, exchange in exchanges.items():
        if pair in exchange.symbols:
            try:
                exchangesBySymbol[pair].append(id)
            except KeyError:
                exchangesBySymbol[pair] = [id]

# Show portfolio
# asyncio.get_event_loop().\
#     run_until_complete(BTC_portfolio_balance(exchanges,arbitrableSymbols))

################################################################################
# Arbitrage
################################################################################

@asyncio.coroutine
def load_prices():
    starttime = time.time()
    counter = 1
    while True and counter < 4:
        asyncio.ensure_future(find_arbitrages(prices, exchanges, exchangesBySymbol))
        counter += 1
        yield from asyncio.sleep(15.0 - ((time.time() - starttime) % 15.0))

task = asyncio.Task(load_prices())
loop = asyncio.get_event_loop()
loop.run_until_complete(task)




# BTC_portfolio_balance(exchanges, Currencies, CurrenciesBitcoinRate)
