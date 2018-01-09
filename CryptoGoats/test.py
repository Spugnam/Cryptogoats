
import os
import sys
import logging
# root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(root)

import math
import json
import datetime
import time
import asyncio
from os import _exit
import ccxt.async as ccxt
# to work with jupyter or from console
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
# Logger initialization
################################################################################

sellExchanges = [] # ['binance', 'bittrex', 'cex']
buyExchanges = [] # ['binance', 'bittrex', 'cex']
# allowedPairs = ['START/BTC', 'STORJ/BTC', 'STORJ/ETH', 'SWT/BTC', 'SYNX/BTC', 'SYS/BTC', 'TX/BTC', 'VIA/BTC', 'VTC/BTC', 'WAVES/BTC', 'WAVES/ETH', 'XEM/BTC', 'XEM/ETH', 'XMG/BTC', 'XRP/BTC', 'XVG/BTC', 'ZEC/BTC', 'ZEC/ETH']
allowedPairs = [] # ['ABY/BTC', 'ADT/BTC']
excludedCurrencies = ['EUR', 'USD', 'GBP', 'AUD', 'JPY', 'CNY']
arbitrage = True
minSpread = 5
min_arb_amount_BTC = .01
max_arb_amount_BTC = .01

################################################################################
# Logger initialization
################################################################################

logPath = "./CryptoGoats/Logs"
time = '{:%Y-%m-%d_%H-%M-%s}'.format(datetime.datetime.now())
fileName = time + "_cryptogoats"

rootLogger = logging.getLogger(__name__)

# add timestamp to each line
logFormatter =\
    logging.Formatter('%(asctime)-2s: %(name)-2s %(levelname)-12s %(message)s',\
                      datefmt='%H:%M')

fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName))
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(level=logging.DEBUG) # log DEBUG to file only
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(level=logging.INFO)
rootLogger.addHandler(consoleHandler)

# helpers module logger
logger.setLevel(level=logging.DEBUG)
logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)

rootLogger.setLevel(level=logging.DEBUG)
rootLogger.info("Created log file: %s", fileName)


################################################################################
# Pairs/ Exchanges Initialization
################################################################################

# create Pandas df
pd.set_option('display.float_format', lambda x: '%.10f' % x) # display 10 digits
prices = pd.DataFrame(columns=['timestamp', 'exchange', 'pair',
                               'exchange_timestamp',
                               'bids', 'high_bid', 'high_bid_size',
                               'asks', 'low_ask', 'low_ask_size', 'volume'])

# Create and connect to all configured exchanges
rootLogger.info("...loading exchanges...")
with open('./CryptoGoats/config_exchanges.json') as f:
    config = json.load(f)

exchanges = {}
for id in ccxt.exchanges:  # list of exchanges id ['acx', bittrex'...]
    if id in config:
        exchange = getattr(ccxt, id) # exchange becomes function bittrex()
        exchanges[id] = exchange(config[id])

# Remove exchanges that couldn't be loaded
notLoaded = list()
for id, exchange in exchanges.items(): # Py3: items() instead of iteritems()
    try:
        _ = asyncio.get_event_loop().run_until_complete(exchange.load_markets(reload=True))
    except:
        notLoaded.append(id)

for id in notLoaded:
    rootLogger.info("Coundn't load %s", id)
    del exchanges[id]

# Find arbitrable pairs (in more than 1 exchange)
# allSymbols = [symbol for _, exchange in exchanges.items() for symbol in exchange.symbols]
rootLogger.info("...loading symbols...")
allSymbols = [symbol for _, exchange in exchanges.items() for symbol in exchange.symbols]
if allowedPairs != []:
    uniqueSymbols = list(set(allSymbols).intersection(set(allowedPairs)))
else:
    uniqueSymbols = list(set(allSymbols))

# filter out symbols that are not present on at least two exchanges
arbitrableSymbols = sorted([symbol for symbol in uniqueSymbols if allSymbols.count(symbol) > 1])

# Remove FIAT es
# TODO make option in program
filteredSymbols = []

for symbol in arbitrableSymbols:
    hasFiat = 0
    for currency in excludedCurrencies:
        if symbol.find(currency) != -1:
            hasFiat = 1
    if hasFiat == 0:
        filteredSymbols.append(symbol)

arbitrableSymbols = filteredSymbols

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

rootLogger.info(style.HEADER + "***Arbitrable Symbols(%d)***" + style.END,\
                len(arbitrableSymbols))
rootLogger.info("%s", arbitrableSymbols)

# Update allowed buy/ sell exchanges and pairs
if sellExchanges == []:
    for id, _ in exchanges.items():
        sellExchanges.append(id)
if buyExchanges == []:
    for id, _ in exchanges.items():
        buyExchanges.append(id)
rootLogger.info(style.OKGREEN + "Allowed exchanges for sell orders %s"\
                + style.END, sellExchanges)
rootLogger.info(style.OKGREEN + "Allowed exchanges for buy orders %s"\
                + style.END, buyExchanges)


# Show portfolio
rootLogger.info("Printing portfolio...")
portfolio = asyncio.get_event_loop().\
    run_until_complete(portfolio_balance(exchanges, arbitrableSymbols, inBTC=False))


'EGC/BTC' in exchanges['cex'].symbols
asyncio.get_event_loop().\
    run_until_complete(exchanges['yobit'].fetch_balance())


BTC_price = asyncio.get_event_loop().\
    run_until_complete(exchanges['gemini'].fetchTicker('BTC/USD'))
BTC_price['ask']

asyncio.get_event_loop().\
    run_until_complete(exchanges['gemini'].fetchTicker('BTC/USD'))['ask']

def test():
    btc = await ccxt.gemini().fetchTicker('BTC/USD')
    print(btc)
    return(btc)

ccxt.yobit().fetch_orders()
ccxt.yobit().fetchTicker('BTC/USD')

test
b

price_in_btc = asyncio.get_event_loop().\
    run_until_complete(exchanges['bittrex'].fetchTicker('ETH/USD'))

price_in_btc = asyncio.get_event_loop().\
    run_until_complete(ccxt.gemini().fetchTicker('BTC/USD'))
price_in_btc

pair = 'ETH/ETH'

if pair.split("/")[1] == 'BTC':
    print(2)
elif pair.split("/")[1] == 'ETH':
    print(3)
else:
    print(4)


price_in_btc



price_in_btc['ask']
price_in_btc

price_in_btc


base_diff = -0.197000
quote_diff = 0.009022
is_higher = base_diff>=-1e-5 and quote_diff>-1e-5
is_higher

orderbook = asyncio.get_event_loop().\
    run_until_complete(exchanges['yobit'].fetch_order_book(pair))

i = 0
while orderbook['bids'][i][1] < 0.3:
    i +=1
orderbook['bids'][i][0]

orderbook['bids'][2]
len(orderbook['bids'])

i = 0
while i < len(orderbook) and\
    orderbook['bids'][i][1] < min_arb_amount_BTC / orderbook['bids'][i][0]:
    logger.debug("min_arb_amount %f bid %f bid size %f, i: %d",\
                 min_arb_amount_BTC / orderbook['bids'][i][0],\
                 high_bid, high_bid_size, i)
    i +=1
