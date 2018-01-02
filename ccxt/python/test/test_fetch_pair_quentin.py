# -*- coding: utf-8 -*-

import argparse
import os
import sys
import json
import time
from os import _exit
from traceback import format_tb

# ------------------------------------------------------------------------------

# root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(root)

# ------------------------------------------------------------------------------

import ccxt  # noqa: E402

# ------------------------------------------------------------------------------

# retrieve rates (last trade) from all exchanges for 1 pair

symbols = [
    'BTC/USD',
    'BTC/CNY',
    'BTC/EUR',
    'BTC/ETH',
    'ETH/BTC',
    'BTC/JPY',
    'LTC/BTC',
    'USD/SLL',
]

# exchange = ccxt.bittrex({
#     "apiKey": "471b47a06c384e81b24072e9a8739064",
#     "secret": "694025686e9445589787e8ca212b4cff",
#     "enableRateLimit": True,
# })

exchange = ccxt.bittrex()

# create all exchanges
exchanges = {}

for id in ccxt.exchanges:
    exchange = getattr(ccxt, id)
    exchanges[id] = exchange()

orderbook = exchange.fetch_order_book('ETH/BTC')

print(orderbook)
print(orderbook['asks'][0][0])  # latest ask price

# print pair for all exchanges
for idb, exchange in exchanges.items():
    try:
        orderbook = exchange.fetch_order_book('ETH/BTC')
        print(orderbook['asks'][0][0])
    except:
        pass


exchanges
