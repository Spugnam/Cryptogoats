import asyncio
import ccxt.async as ccxt
import time
import math
import pandas as pd
from collections import defaultdict

import logging
logger = logging.getLogger(__name__)
logger.propagate = False # only use the output from rootLogger


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


async def find_arbitrage(df, pair, exchanges, exchangesBySymbol):
    """ Cycles once through all pairs and exchanges and triggers arb trades
    """

    # Load orderbooks at all exchanges containing pair
    for id in exchangesBySymbol[pair]:
        row = await load_order_book(exchanges[id], pair)
        df = df.append(pd.DataFrame([row], columns=df.columns)\
                            , ignore_index=True)

    # print biggest spread for pair
    bb_exchange, best_bid, best_bid_size,\
        ba_exchange, best_ask, best_ask_size, spread = get_spread(pair, df)
    logger.info("Biggest percent spread for %s: %s", pair, spread)

    if spread > 5:

        # Min 0.01 ~ 160 USD
        min_arb_amount = 0.01 / best_bid
        # Minimal trade value on cex
        min_arb_amount = max(min_arb_amount, 0.1)

        # Max 0.07 ~ 1000 USD
        max_arb_amount = max(0.01 / best_bid, min_arb_amount)

        # Check funds
        bb_balance = await exchanges[bb_exchange].fetch_balance()
        ba_balance = await exchanges[ba_exchange].fetch_balance()
        arb_amount = min(max_arb_amount, best_bid_size, best_ask_size)

        # Rounding below needed for gdax
        # Will also loosen the limit and trigger more executions
        sell_price = math.floor(best_bid*0.999 * 1e5) / 1e5
        buy_price = math.ceil(best_ask*1.001 * 1e5) / 1e5


        # reduce arb_amount to what funds allow
        arb_amount = min(bb_balance[pair.split("/")[0]]['total'],\
                         ba_balance[pair.split("/")[1]]['total']/buy_price,\
                         arb_amount)

        # Check orderbook size
        if (min_arb_amount <= best_bid_size and min_arb_amount <= best_ask_size):

            if (arb_amount >= min_arb_amount):

                # Round amount to 4 digits
                arb_amount = round(arb_amount, 4)
                logger.info("***** arbitrage *****")
                # take

                logger.info("Amount in USD: %f", arb_amount * 15000 * best_bid)
                # print("Amount in USD",\
                #       arb_amount * 15000 * best_bid) # TODO get BTC price in USD
                logger.info("Sell %s %s amount: %f %f", bb_exchange, pair, arb_amount, sell_price)
                # print("Sell", bb_exchange, pair, "amount", arb_amount, sell_price)
                logger.info("Buy %s %s amount: %f %f", ba_exchange, pair, arb_amount, buy_price)
                # print("Buy", ba_exchange, pair, "amount", arb_amount, buy_price)

                # Launch orders
                sell_order = await exchanges[bb_exchange].create_limit_sell_order(pair, arb_amount, sell_price)
                logger.info("Sell order: %s", sell_order)
                # print("Sell order: ", sell_order)
                buy_order = await exchanges[ba_exchange].create_limit_buy_order(pair, arb_amount, buy_price)
                logger.info("Buy order: %s", buy_order)
                # print("Buy order: ", buy_order)
                # print("\n")

                # Check portfolio went up
                bb_balance_after = await exchanges[bb_exchange].fetch_balance()
                ba_balance_after = await exchanges[ba_exchange].fetch_balance()

                portfolio_up = False
                portfolio_counter = 0
                while portfolio_up == False and portfolio_counter < 3:
                    portfolio_up = await is_higher(pair.split("/")[0], pair.split("/")[1],\
                                             bb_balance, ba_balance,\
                                             bb_balance_after, ba_balance_after)
                    time.sleep(5)
                    portfolio_counter += 1
                return(portfolio_up)
            else:
                logger.info("  Not enough funds")
                logger.debug("  sell balance %s %s %f", bb_exchange, pair.split("/")[0],
                      bb_balance[pair.split("/")[0]]['total'])
                # print("  sell balance", bb_exchange, pair.split("/")[0],
                #       bb_balance[pair.split("/")[0]]['total'])
                logger.debug("  Minimum amount %f", min_arb_amount)
                # print("  amount to sell", arb_amount)
                logger.debug("  buy balance %s %s %f", ba_exchange, pair.split("/")[1],
                      ba_balance[pair.split("/")[1]]['total'])
                # print("  buy balance", ba_exchange, pair.split("/")[1],
                #       ba_balance[pair.split("/")[1]]['total'])
                logger.debug("  Minimum amount %f", min_arb_amount * buy_price)
                # print("  amount to buy", arb_amount * buy_price)

        else:
            logger.info("  No order book size")
            logger.debug("  amount to sell %f bid_size %f", min_arb_amount, best_bid_size)
            logger.debug("  amount to buy %f ask_size %f", min_arb_amount, best_ask_size)

    return(True) # nothing was done

################################################################################
# Portfolio Balance
################################################################################

async def is_higher(base, quote, bb_balance, ba_balance,\
                         bb_balance_after, ba_balance_after):

    base_diff = bb_balance_after[base]['total'] - bb_balance[base]['total'] +\
        ba_balance_after[base]['total'] - ba_balance[base]['total']
    logger.info("Portfolio change: %f %s", base_diff, base)
    quote_diff = bb_balance_after[quote]['total'] - bb_balance[quote]['total'] +\
        ba_balance_after[quote]['total'] - ba_balance[quote]['total']
    logger.info("Portfolio change: %f %s", quote_diff, quote)

    return(base_diff>=-1e-5 and quote_diff>-1e-5)

async def portfolio_balance(exchanges, arbitrableSymbols, inBTC=False):
    """ Returns the value of the portfolio in BTC for all currencies in
    arbitrable pairs
    """

    portfolio = defaultdict()

    # Dictionary to store currency rates in BTC
    Currencies = list()
    for pair in arbitrableSymbols:
        Currencies.append(pair.split('/')[0])
    Currencies = list(set(Currencies))

    if inBTC:
        logger.info("...loading BTC rates...")
        CurrenciesBitcoinRate = dict()
        for curr in Currencies:
            CurrenciesBitcoinRate[curr] =\
                await exchanges['bittrex'].fetchTicker(curr + '/BTC')


    Currencies.append('BTC')

    BTC_value = 0
    for id, exchange in exchanges.items():
        balance = await exchange.fetch_balance()
        logging.info('\n')
        logger.info("Exchange: %s", id)
        for curr in Currencies:
            try:
                logger.info("%s %f", curr, balance[curr]['total'])
                try:
                    portfolio[curr] += balance[curr]['total']
                except KeyError:
                    portfolio[curr] = balance[curr]['total']
            except:
                pass # currency not in exchange

    # Print totals by Currencies
    logging.info('\n')
    logger.info("Currency totals")
    for curr, total in portfolio.items():
        logger.info("%s %f", curr, total)

    if inBTC:
        for curr, total in portfolio.items():
            if curr == 'BTC':
                BTC_value += portfolio[curr] * 1.
            else:
                BTC_value += portfolio[curr] * CurrenciesBitcoinRate[curr]['ask']
        logger.info("Total portfolio balance: %f BTC", BTC_value)
    return(portfolio)

################################################################################
# Refill cex with BTC
################################################################################

def get_spread_refill(pair, df, exchange_refill):
    """ Identify spread for a pair and returns data for arbitrage trade
    """
    bb_exchange = 'bittrex'
    best_bid = df.loc[df['exchange'] == bb_exchange, 'high_bid'].values[0]
    best_bid_size = df.loc[df['exchange'] == bb_exchange, 'high_bid_size'].values[0]

    ba_exchange = exchange_refill
    best_ask = df.loc[df['exchange'] == ba_exchange, 'low_ask'].values[0]
    best_ask_size = df.loc[df['exchange'] == ba_exchange, 'low_ask_size'].values[0]


    spread = 100 * (best_bid - best_ask) / best_bid

    return(bb_exchange, best_bid, best_bid_size,\
           ba_exchange, best_ask, best_ask_size, spread)


async def find_arbitrage_refill(df, pair, exchanges, exchangesBySymbol, exchange_refill):
    """ Cycles once through all pairs and exchanges and triggers arb trades
    """

    # Load orderbooks at all exchanges containing pair
    for id in exchangesBySymbol[pair]:
        row = await load_order_book(exchanges[id], pair)
        df = df.append(pd.DataFrame([row], columns=df.columns)\
                            , ignore_index=True)

    # print biggest spread for pair
    bb_exchange, best_bid, best_bid_size,\
        ba_exchange, best_ask, best_ask_size, spread = get_spread_refill(pair, df, exchange_refill)
    print("Biggest % spread for ", pair, spread, "sell at", bb_exchange, "buy at", ba_exchange)

    if spread > 1.5:

        # 0.017 btc is 250 USD
        min_arb_amount = 0.017 / best_bid
        # Minimal trade value on cex
        min_arb_amount = max(min_arb_amount, 0.1)

        max_arb_amount = max(0.07 / best_bid, min_arb_amount)

        # Check funds
        bb_balance = await exchanges[bb_exchange].fetch_balance()
        ba_balance = await exchanges[ba_exchange].fetch_balance()
        # min_funds = min(bb_balance, ba_balance)
        # max_arb_amount = min(max_arb_amount, bb_balance)

        arb_amount = min(max_arb_amount, best_bid_size, best_ask_size)

        # Check orderbook size
        if (min_arb_amount <= best_bid_size and min_arb_amount <= best_ask_size):

            sell_price = math.floor(best_bid*0.999 * 1e5) / 1e5
            buy_price = math.ceil(best_ask*1.001 * 1e5) / 1e5

            if (bb_balance[pair.split("/")[0]]['total'] > arb_amount) and\
                (ba_balance[pair.split("/")[1]]['total'] > arb_amount * buy_price):

                # Round amount to 4 digits (prevent "amount too precise")
                arb_amount = math.floor(arb_amount *1e4) / 1e4
                print("***** arbitrage *****")
                # take
                print("Amount in USD",\
                      arb_amount * 15000 * best_bid) # TODO get BTC price in USD
                print("Sell", bb_exchange, pair, "amount", arb_amount, sell_price)
                print("Buy", ba_exchange, pair, "amount", arb_amount, buy_price)

                # Launch orders
                sell_order = await exchanges[bb_exchange].create_limit_sell_order(pair, arb_amount, sell_price)
                print("Sell order: ", sell_order)
                buy_order = await exchanges[ba_exchange].create_limit_buy_order(pair, arb_amount, buy_price)
                print("Buy order: ", buy_order)
                print("\n")

                # Check portfolio went up
                portfolio_up = False
                portfolio_counter = 0

                while portfolio_up == False and portfolio_counter < 3:
                    bb_balance_after = await exchanges[bb_exchange].fetch_balance()
                    ba_balance_after = await exchanges[ba_exchange].fetch_balance()

                    portfolio_up = await is_higher(pair.split("/")[0], pair.split("/")[1],\
                                             bb_balance, ba_balance,\
                                             bb_balance_after, ba_balance_after)
                    time.sleep(5)
                    portfolio_counter += 1
                return(portfolio_up)
            else:
                print("  Not enough funds")
                print("  sell balance", bb_exchange, pair.split("/")[0],
                      bb_balance[pair.split("/")[0]]['total'])
                print("  amount to sell", arb_amount)
                print("  buy balance", ba_exchange, pair.split("/")[1],
                      ba_balance[pair.split("/")[1]]['total'])
                print("  amount to buy", arb_amount * buy_price)

        else:
            print("  No order book size")
            print("  amount to sell", min_arb_amount,"bid_size", best_bid_size)
            print("  amount to buy", min_arb_amount, "ask_size", best_ask_size)

    return(True) # nothing was done
