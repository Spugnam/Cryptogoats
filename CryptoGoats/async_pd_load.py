
import os
import sys
# root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(root)

import math
import json
import time
import asyncio
from os import _exit
import ccxt
# TODO fix below
try:
    from CryptoGoats.helpers import get_spread
except:
    pass
try:
    from helpers import get_spread
except:
    pass
import psycopg2
import pandas as pd


########################################################################
# Initialization
########################################################################

# create Pandas df
pd.set_option('display.float_format', lambda x: '%.10f' % x) # display 10 digits
prices = pd.DataFrame(columns=['timestamp', 'exchange', 'pair',
                               'exchange_timestamp',
                               'bids', 'high_bid', 'high_bid_size',
                               'asks', 'low_ask', 'low_ask_size', 'volume'])
# convert columns to float
# prices[['high_bid','low_ask']] = prices[['high_bid','low_ask']].apply(pd.to_numeric)

# Create and connect to all configured exchanges
with open('./CryptoGoats/config_exchanges.json') as f:
    config = json.load(f)

exchanges = {}
for id in ccxt.exchanges:  # list of exchanges id ['acx', bittrex'...]
    if id in config:
        exchange = getattr(ccxt, id) # exchange becomes function bittrex()
        exchanges[id] = exchange(config[id])

# Load all markets
for id, exchange in exchanges.items(): # Py3: items() instead of iteritems()
    try:
        _ = exchange.load_markets(reload=True)
    except:
        pass

# Find arbitrable pairs (in more than 1 exchange)
allSymbols = [symbol for _, exchange in exchanges.items() for symbol in exchange.symbols]
uniqueSymbols = list(set(allSymbols))
# filter out symbols that are not present on at least two exchanges
arbitrableSymbols = sorted([symbol for symbol in uniqueSymbols if allSymbols.count(symbol) > 1])

# Remove FIAT currencies
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
from collections import defaultdict
exchangesBySymbol = defaultdict()

for pair in arbitrableSymbols:
    for id, exchange in exchanges.items():
        if pair in exchange.symbols:
            try:
                exchangesBySymbol[pair].append(id)
            except KeyError:
                exchangesBySymbol[pair] = [id]

# Dictionary to store currency rates in BTC
CurrenciesBitcoinRate = dict()
# Initial load - only works for XXX/BTC pairs
Currencies = list()
for pair in arbitrableSymbols:
    Currencies.append(pair.split('/')[0])
Currencies = list(set(Currencies))

print("...loading BTC rates...")
for curr in Currencies:
    CurrenciesBitcoinRate[curr] = exchanges['bittrex'].fetchTicker(curr + '/BTC')['ask']

# Arb Trades###############################
#exchange['bittrex'].create_limit_buy_order('BTC/XRP', amount, price)

# Create async versions of orders functions

async def async_create_limit_sell_order(exchange, pair, amount, limit):
    return exchange.create_limit_sell_order(pair, amount, limit)

async def async_create_limit_buy_order(exchange, pair, amount, limit):
    return exchange.create_limit_buy_order(pair, amount, limit)


async def create_arb_trade(pair, amount, bb_exchange,
                     sell_price, ba_exchange, buy_price, spread):

    print("sell", bb_exchange, pair, "amount", amount, sell_price)
    print("buy", ba_exchange, pair, "amount", amount, buy_price)

    sell_order = await async_create_limit_sell_order(exchanges[bb_exchange], pair, amount, sell_price)
    print("Sell order: ", sell_order)
    buy_order = await async_create_limit_buy_order(exchanges[ba_exchange], pair, amount, buy_price)
    print("Buy order: ", buy_order)

async def load_arbitrableSymbols(df):

    global CurrenciesBitcoinRate

    for pair in arbitrableSymbols:
        for id in exchangesBySymbol[pair]:
            row = await load_order_book(exchanges[id], pair)
            df = df.append(pd.DataFrame([row], columns=df.columns)\
                                , ignore_index=True)
        # print biggest spread for pair
        bb_exchange, best_bid, best_bid_size,\
            ba_exchange, best_ask, best_ask_size, spread = get_spread(pair, df)

        # Store BTC rate
        CurrenciesBitcoinRate[pair.split("/")[0]] = best_bid

        print("Biggest % spread for ", pair, spread)

        if spread > 0.6:

            # about 150 USD
            arb_amount = 0.017 / best_bid

            # Minimal trade value on cex
            arb_amount = max(arb_amount, 0.1)

            # Check if orderbook allows trade
            if (arb_amount <= best_bid_size and arb_amount <= best_ask_size):

                sell_price = math.floor(best_bid*0.999 * 1e5) / 1e5
                buy_price = math.ceil(best_ask*1.001 * 1e5) / 1e5



                if (exchanges[bb_exchange].fetch_balance()[pair.split("/")[0]]['total'] > arb_amount) and\
                    (exchanges[ba_exchange].fetch_balance()[pair.split("/")[1]]['total'] > arb_amount * buy_price):

                    # Round amount to 4 digits
                    arb_amount = round(arb_amount, 4)
                    print("***** arbitrage *****")
                    print("Amount in USD",\
                          arb_amount * 15000 * best_bid) # TODO get BTC price in USD
                    await create_arb_trade(pair, arb_amount, bb_exchange, best_bid,
                                     ba_exchange, best_ask, spread)
                    print("\n")
                else:
                    print("  Not enough funds")

                    print("  sell balance", bb_exchange, pair.split("/")[0],
                          exchanges[bb_exchange].fetch_balance()[pair.split("/")[0]]['total'])
                    print("  amount to sell", arb_amount)

                    print("  buy balance", ba_exchange, pair.split("/")[1],
                          exchanges[bb_exchange].fetch_balance()[pair.split("/")[1]]['total'])
                    print("  amount to buy", arb_amount * buy_price)

            else:
                print("  No order book size", "best_bid_size", best_bid_size, "best_ask_size", best_ask_size)


########################################################################
# Portfolio Balance

def BTC_portfolio_balance(exchanges, arbitrableSymbols):
    """ Returns the value of the portfolio in BTC for all currencies in
    arbitrable pairs
    """
    global CurrenciesBitcoinRate
    global Currencies

    BTC_value = 0
    for id, exchange in exchanges.items():
        balance = exchange.fetch_balance()
        print("\n")
        print("Exchange:", id)
        print('BTC', balance['BTC']['total'])
        BTC_value += balance['BTC']['total']
        for curr in Currencies:
            try:
                print(curr, balance[curr]['total'])
                BTC_value +=\
                (balance[curr]['total'] * CurrenciesBitcoinRate[curr])
            except:
                pass # currency not in exchange

    print("Total portfolio balance: ", BTC_value, "(BTC)")

########################################################################
# Download dataframe to psql

# # Connect to PostgreSQL database
# with open('./PostgreSQL/config_psql.json') as f:
#     config = json.load(f)
#
# conn = psycopg2.connect(**config)

########################################################################
# Load prices infinite loop

@asyncio.coroutine
def load_prices():
    starttime = time.time()
    counter = 1
    while True and counter < 2:
        asyncio.ensure_future(load_arbitrableSymbols(prices))
        counter += 1
        yield from asyncio.sleep(15.0 - ((time.time() - starttime) % 15.0))

task = asyncio.Task(load_prices())
loop = asyncio.get_event_loop()
loop.run_until_complete(task)



BTC_portfolio_balance(exchanges, arbitrableSymbols)



# exchanges['bittrex'].fetch_order('75d1d050-f325-4755-87c8-3dbbe35b6820')


# exchanges['gdax'].fetch_order('a0801d31-e2a7-4545-988a-3d747e0b08f6')

# exchanges['bittrex'].create_limit_sell_order('XRP/BTC__', 350, 0.000143)


# exchanges['cex'].uid
# exchanges['cex'].create_limit_sell_order('BCH/BTC', 0.0001, 0.17)
# first currency (base pair) action on it and amount in it

# {'id': '75d1d050-f325-4755-87c8-3dbbe35b6820',
#  'info': {'message': '',
#   'result': {'uuid': '75d1d050-f325-4755-87c8-3dbbe35b6820'},
#   'success': True}}

# exchanges['bittrex'].fetch_order_book('XRP/BTC')
# exchanges['bittrex'].symbols




# exchanges['bittrex'].fetch_balance()['BCH']['total']
# exchanges['cex'].symbols

# Check balance
exchanges['bittrex'].fetch_balance()
# exchanges['gdax'].fetch_balance()
exchanges['gemini'].fetch_balance()
# exchanges['cex'].fetch_balance()


# exchanges['bittrex'].fetch_balance()['BTC']
# exchanges['bittrex'].fetch_balance()['XRP']
# # exchanges['bittrex'].fetch_balance()['LTC']
# # exchanges['gdax'].fetch_balance()['BTC']
# # exchanges['gdax'].fetch_balance()['LTC']
# # exchanges['cex'].fetch_balance()['XRP']
# # exchanges['bittrex'].fetch_balance()
#
# exchanges['bittrex'].fetch_balance()['XRP']
# exchanges['cex'].fetch_balance()['XRP']
#
# exchanges['bittrex'].fetchOrder('72ed90b5-6d9a-4408-8d28-a3c93a502bbe')




# Biggest % spread for  ZEC/BTC -0.546614520863

# exchanges['gdax'].fetchTicker('BTC/USD')

# exchanges['bittrex'].fetchTicker('XRP/BTC')['ask']
# exchanges['bittrex'].fetchTicker('BTG/BTC')['ask']
# exchanges['gdax'].fetchTicker('XRP/USD')['info']['price']


# Tests
# prices[prices['pair']=='NEO/ETH']
