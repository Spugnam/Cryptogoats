
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
    from CryptoGoats.trading_functions import *
except:
    pass
try:
    from trading_functions import *
except:
    pass
import psycopg2
import pandas as pd

################################################################################
# Logger initialization
################################################################################

sellExchanges = [] # ['binance', 'bittrex', 'cex']
buyExchanges = [] # ['binance', 'bittrex', 'cex']
allowedExchanges = ['bittrex', 'cex', 'gdax', 'yobit'] # ['bittrex', 'cex', 'gdax', 'yobit'] #
allowedPairs = ['LSK/BTC', 'VIA/BTC','STORJ/BTC','BCH/BTC','ETH/BTC','ZEC/BTC','XRP/BTC','ZEC/BTC', 'BTG/BTC','LTC/BTC','DASH/BTC']
excludedCurrencies = ['EUR', 'USD', 'GBP', 'AUD', 'JPY', 'CNY']
arbitrage = True
minSpread = 1
min_arb_amount_BTC = .004
max_arb_amount_BTC = .07
displayPortolio = True
cycles = 15 # number of cycles through all available pairs
loggingMode = logging.INFO # logging.DEBUG, logging.INFO
inBTC = False # Display portfolio value in BTC

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
consoleHandler.setLevel(level=loggingMode)
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

if allowedExchanges == []:
    for id in config:
        allowedExchanges.append(id)

exchanges = {}
for id in ccxt.exchanges:  # list of exchanges id ['acx', bittrex'...]
    if id in allowedExchanges:
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
    rootLogger.info(style.FAIL + "Coundn't load %s" + style.END, id)
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


################################################################################
# Arbitrage
################################################################################


# Show portfolio
if displayPortolio:
    portfolio = asyncio.get_event_loop().\
        run_until_complete(portfolio_balance(exchanges, arbitrableSymbols,\
                                             inBTC=inBTC))


@asyncio.coroutine
def main():
    # starttime = time.time()
    counter = 1
    pair_counter = 0
    while True and counter <= cycles*len(arbitrableSymbols):
        try:
            pair = arbitrableSymbols[pair_counter]
            result =\
            yield from pair_arbitrage(prices,\
                                     pair,\
                                     exchanges,\
                                     exchangesBySymbol,\
                                     sellExchanges,\
                                     buyExchanges,\
                                     arbitrage=arbitrage,\
                                     minSpread=minSpread,\
                                     min_arb_amount_BTC = min_arb_amount_BTC,\
                                     max_arb_amount_BTC = max_arb_amount_BTC)
            if result == -1:
                rootLogger.info(style.FAIL + "Interrupting" + style.END )
                break
            counter += 1
            if result == 0: # stay on same pair if arbitrage was made
                pair_counter = (pair_counter + 1) % (len(arbitrableSymbols))
        except KeyboardInterrupt:
            rootLogger.info("exiting program (print portfolio here)")
    newPortfolio = yield from portfolio_balance(exchanges,
                                                arbitrableSymbols)
    rootLogger.info(style.OKBLUE + "Portfolio Change summary" + style.END)
    for curr, total in newPortfolio.items():
        rootLogger.info("%s %f", curr,\
                        newPortfolio[curr] - portfolio[curr])

task = asyncio.Task(main())
loop = asyncio.get_event_loop()
loop.run_until_complete(task)


# Test transfer
# transfer = asyncio.get_event_loop().\
#     run_until_complete(exchanges['bittrex'].withdraw('XRP', 10, 'rE1sdh25BJQ3qFwngiTBwaq3zPGGYcrjp1', params = {'tag': 28577}))

# transfer

# Check orders
# orders_cex = asyncio.get_event_loop().\
#     run_until_complete( exchanges['cex'].fetchOrders())
#
# orders = asyncio.get_event_loop().\
#     run_until_complete( exchanges['bittrex'].fetchOrders())
#
# orders

# balb = asyncio.get_event_loop().\
#     run_until_complete( exchanges['bittrex'].fetch_balance())
# balb

# bal = asyncio.get_event_loop().\
#     run_until_complete( exchanges['cex'].fetch_balance())
# bal
# transfer = asyncio.get_event_loop().\
#     run_until_complete(exchanges['gdax'].\
#                        withdraw('LTC', .05, 'LaW2wZKqfxaHWjTC7EQV4f39KZWNrZMbbQ', params = {}))
#
# transfer
