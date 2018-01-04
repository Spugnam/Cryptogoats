import asyncio
import ccxt.async as ccxt
import time
import math
import pandas as pd



################################################################################
# OrderBook
################################################################################

async def load_order_book(exchange, pair):
    """ Add order_book to Pandas dataframe
    pair = 'ETH/BTC'
    """
    orderbook = await exchange.fetch_order_book(pair)

    row = [time.time(), exchange.id, pair, orderbook['timestamp'],\
     ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['bids'][:5])),\
     orderbook['bids'][0][0], # highest bid
     orderbook['bids'][0][1], # highest bid size
    ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['asks'][:5])),\
    orderbook['asks'][0][0], # lowest ask
    orderbook['asks'][0][1], # lowest ask size
    1000]

    return(row)

################################################################################
# Arbitrage
################################################################################

def get_spread(pair, df):
    """ Identify spread for a pair and returns data for arbitrage trade
    """
    best_bid = df.loc[df['pair'] == pair, ['high_bid']].unstack().max()
    bb_idx = df.loc[df['pair'] == pair, ['high_bid']].idxmax()
    best_bid_size = df.loc[bb_idx, 'high_bid_size'].values[0]
    bb_exchange = df.loc[bb_idx, 'exchange'].values[0]

    best_ask = df.loc[df['pair'] == pair, ['low_ask']].unstack().min()
    ba_idx = df.loc[df['pair'] == pair, ['low_ask']].idxmin()
    best_ask_size = df.loc[ba_idx, 'low_ask_size'].values[0]
    ba_exchange = df.loc[ba_idx, 'exchange'].values[0]

    spread = 100 * (best_bid - best_ask) / best_bid

    return(bb_exchange, best_bid, best_bid_size,\
           ba_exchange, best_ask, best_ask_size, spread)

# Create async versions of orders functions

async def async_create_limit_sell_order(exchange, pair, amount, limit):
    return exchange.create_limit_sell_order(pair, amount, limit)

async def async_create_limit_buy_order(exchange, pair, amount, limit):
    return exchange.create_limit_buy_order(pair, amount, limit)


async def create_arb_trade(pair, amount, bb_exchange,
                     sell_price, ba_exchange, buy_price, spread):

    print("Sell", bb_exchange, pair, "amount", amount, sell_price)
    print("Buy", ba_exchange, pair, "amount", amount, buy_price)

    sell_order = await async_create_limit_sell_order(exchanges[bb_exchange], pair, amount, sell_price)
    print("Sell order: ", sell_order)
    buy_order = await async_create_limit_buy_order(exchanges[ba_exchange], pair, amount, buy_price)
    print("Buy order: ", buy_order)


async def find_arbitrages(df, exchanges, exchangesBySymbol):
    """ Cycles once through all pairs and exchanges and triggers arb trades
    """

    for pair, _ in exchangesBySymbol.items():
        for id in exchangesBySymbol[pair]:
            row = await load_order_book(exchanges[id], pair)
            df = df.append(pd.DataFrame([row], columns=df.columns)\
                                , ignore_index=True)
        # print biggest spread for pair
        bb_exchange, best_bid, best_bid_size,\
            ba_exchange, best_ask, best_ask_size, spread = get_spread(pair, df)

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

                bb_exchange_balance = await exchanges[bb_exchange].fetch_balance()
                ba_exchange_balance = await exchanges[ba_exchange].fetch_balance()

                if (bb_exchange_balance[pair.split("/")[0]]['total'] > arb_amount) and\
                    (ba_exchange_balance[pair.split("/")[1]]['total'] > arb_amount * buy_price):

                    # Round amount to 4 digits
                    arb_amount = round(arb_amount, 4)
                    print("***** arbitrage *****")
                    # take
                    print("Amount in USD",\
                          arb_amount * 15000 * best_bid) # TODO get BTC price in USD
                    await create_arb_trade(pair, arb_amount, bb_exchange, best_bid,
                                     ba_exchange, best_ask, spread)
                    print("\n")
                else:
                    print("  Not enough funds")
                    print("  sell balance", bb_exchange, pair.split("/")[0],
                          bb_exchange_balance[pair.split("/")[0]]['total'])
                    print("  amount to sell", arb_amount)
                    print("  buy balance", ba_exchange, pair.split("/")[1],
                          ba_exchange_balance[pair.split("/")[1]]['total'])
                    print("  amount to buy", arb_amount * buy_price)

            else:
                print("  No order book size")
                print("  amount to sell", arb_amount,"bid_size", best_bid_size)
                print("  amount to buy", arb_amount, "ask_size", best_ask_size)

################################################################################
# Portfolio Balance
################################################################################

async def BTC_portfolio_balance(exchanges, arbitrableSymbols, inBTC=False):
    """ Returns the value of the portfolio in BTC for all currencies in
    arbitrable pairs
    """

    # Dictionary to store currency rates in BTC
    # Only works for XXX/BTC pairs
    Currencies = list()
    for pair in arbitrableSymbols:
        Currencies.append(pair.split('/')[0])
    Currencies = list(set(Currencies))

    if inBTC:
        print("...loading BTC rates...")
        CurrenciesBitcoinRate = dict()
        for curr in Currencies:
            CurrenciesBitcoinRate[curr] =\
                await exchanges['bittrex'].fetchTicker(curr + '/BTC')

    BTC_value = 0
    for id, exchange in exchanges.items():
        balance = await exchange.fetch_balance()
        print("\n")
        print("Exchange:", id)
        print('BTC', balance['BTC']['total'])
        BTC_value += balance['BTC']['total']
        for curr in Currencies:
            try:
                print(curr, balance[curr]['total'])
                if inBTC:
                    BTC_value +=\
                    (balance[curr]['total'] * CurrenciesBitcoinRate[curr]['ask'])
            except:
                pass # currency not in exchange

    if inBTC:
        print("Total portfolio balance: ", BTC_value, "(BTC)")
