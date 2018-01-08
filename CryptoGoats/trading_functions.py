import asyncio
import ccxt.async as ccxt
import time
import math
import numbers
import pandas as pd
from collections import defaultdict

import logging
logger = logging.getLogger(__name__)
logger.propagate = False # only use the output from rootLogger

class style:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    YELLOW = '\033[93m'
    LIGHTBLUE = '\033[96m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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

def get_spread(pair, df, sellExchanges, buyExchanges):
    """ Identify spread for a pair and returns data for arbitrage trade
    """
    best_bid = df.loc[(df['pair'] == pair) &\
                      (df['exchange'].isin(sellExchanges)),\
                      ['high_bid']].unstack().max()
    try:
        assert isinstance(best_bid, numbers.Number)
    except AssertionError:
        logger.debug("%s", df.loc[df['pair']])

    try:
        bb_idx = df.loc[(df['pair'] == pair) &\
                      (df['exchange'].isin(sellExchanges)), ['high_bid']].idxmax()
    except Exception:
        logger.error("Catching %s", df.loc[df['pair'] == pair], exc_info=True)
    best_bid_size = df.loc[bb_idx, 'high_bid_size'].values[0]
    bb_exchange = df.loc[bb_idx, 'exchange'].values[0]

    best_ask = df.loc[(df['pair'] == pair) &\
                      (df['exchange'].isin(buyExchanges)),\
                      ['low_ask']].unstack().min()
    # try:
    ba_idx = df.loc[(df['pair'] == pair) &\
                      (df['exchange'].isin(buyExchanges)), ['low_ask']].idxmin()
    # except Exception:
    #     logger.error("%s", df.loc[df['pair'] == pair], exc_info=True)
    best_ask_size = df.loc[ba_idx, 'low_ask_size'].values[0]
    ba_exchange = df.loc[ba_idx, 'exchange'].values[0]

    spread = 100 * (best_bid - best_ask) / best_bid

    logger.debug("best bid: %f, best bid size: %f (%s)",\
                 best_bid, best_bid_size, bb_exchange)
    logger.debug("best ask: %f, best ask size: %f (%s)",\
                 best_ask, best_ask_size, ba_exchange)

    return(bb_exchange, best_bid, best_bid_size,\
           ba_exchange, best_ask, best_ask_size, spread)


async def pair_arbitrage(df, pair, exchanges, exchangesBySymbol,\
                         sellExchanges, buyExchanges,\
                         arbitrage=False, minSpread=5,\
                         min_arb_amount_BTC=0.01, max_arb_amount_BTC=.01):
    """ 1) Calculate best spread for a pair at available exchanges
        2) Performs check and create arbitrage orders
    Returns True if
        -nothing was done
        or
        -arbitrage was attempted and portfolio is not down
        (allowing for rounding)
    """
    # For USD calculations
    BTC_price = await ccxt.gemini().fetchTicker('BTC/USD')
    ############################################################
    # Load orderbooks at all exchanges containing pair
    ############################################################

    for id in exchangesBySymbol[pair]:
        try:
            logger.debug("Exchange: %s pair: %s", id, pair)
            row = await load_order_book(exchanges[id], pair)
            df = df.append(pd.DataFrame([row], columns=df.columns)\
                            , ignore_index=True)
        except:
            pass # e.g. order book empty

    # Find biggest spread for pair
    try:
        bb_exchange, best_bid, best_bid_size,\
            ba_exchange, best_ask, best_ask_size, spread =\
                get_spread(pair, df, sellExchanges, buyExchanges)

        spreadString = "Biggest percent spread for %s: %s (sell %s, buy %s)"\
                % (pair, spread, bb_exchange, ba_exchange)
        if spread > minSpread:
            logger.info(style.BOLD + spreadString + style.END)
        else:
            logger.info(spreadString)

    except Exception as mess:
        logger.warning(style.FAIL + "%s" + style.END, mess)
        logger.error("No spread calculated for %s at %s", pair, id, exc_info=True)
        logger.debug("%s", df.loc[df['pair'] == pair], exc_info=True)
        return(True) # nothing was done

    ############################################################
    # Checks and Arbitrage
    ############################################################

    if not (arbitrage and spread > minSpread):
        return(True) # nothing was done

    # Min amount in base currency
    min_arb_amount = min_arb_amount_BTC / best_bid
    # min_arb_amount = max(min_arb_amount, 0.1) # Minimal trade value on cex
    # Max amount in base currency
    max_arb_amount = max_arb_amount_BTC / best_bid
    max_arb_amount = max(max_arb_amount, min_arb_amount)
    # Possible amount per order book
    arb_amount = min(max_arb_amount, best_bid_size, best_ask_size)
    logger.debug("Arb amount after orderbook adjustment %f", arb_amount)

    # Check funds
    for _ in range(3):
        try:
            bb_balance = await exchanges[bb_exchange].fetch_balance()
            ba_balance = await exchanges[ba_exchange].fetch_balance()
        except Exception as mess:
            logger.warning(style.FAIL + "%s" + style.END, mess)
        else:
            break
    if bb_balance is None:
        logger.warning(style.FAIL + "Balance couldn't be retrieved%s"\
                       + style.END, bb_exchange)
        return(True)
    if ba_balance is None:
        logger.warning(style.FAIL + "Balance couldn't be retrieved%s"\
                       + style.END, ba_balance)
        return(True)

    # Rounding below needed for gdax
    # Will also loosen the limit and trigger more executions
    sell_price = math.floor(best_bid*0.999 * 1e5) / 1e5
    buy_price = math.ceil(best_ask*1.001 * 1e5) / 1e5

    # Check wallet exist, reduce arb_amount to what funds allow
    try:
        arb_amount = min(bb_balance[pair.split("/")[0]]['total'],\
                         ba_balance[pair.split("/")[1]]['total']/buy_price,\
                         arb_amount)
        logger.debug("Arb amount after funds check %f", arb_amount)
        logger.debug("min_arb_amount %f", min_arb_amount)
    except Exception as mess:
        logger.warning(style.LIGHTBLUE + "No wallet defined %s" + style.END, mess)
        return(True)

    # Check orderbook size
    if not (min_arb_amount <= best_bid_size and\
            min_arb_amount <= best_ask_size):
        logger.warning("  No order book size")
        logger.info("  amount to sell %f bid_size %f", min_arb_amount, best_bid_size)
        logger.info("  amount to buy %f ask_size %f", min_arb_amount, best_ask_size)
        return(True)

    # Check enough funds
    if not (arb_amount >= min_arb_amount):
        logger.warning("  Not enough funds")
        logger.info("  sell balance %s %s %f", bb_exchange, pair.split("/")[0],
              bb_balance[pair.split("/")[0]]['total'])
        logger.info("  Minimum amount %f", min_arb_amount)
        logger.info("  buy balance %s %s %f", ba_exchange, pair.split("/")[1],
              ba_balance[pair.split("/")[1]]['total'])
        logger.info("  Minimum amount %f", min_arb_amount * buy_price)
        return(True)


    # Prevent "order too precise" error
    arb_amount = math.floor(arb_amount *1e4) / 1e4

    # Arbitrage
    logger.info(style.BOLD + "***** arbitrage *****" + style.END)
    logger.info("Amount in USD: %f", arb_amount * BTC_price['ask'] * best_bid)
    # print("Amount in USD",\
    #       arb_amount * 15000 * best_bid) # TODO get BTC price in USD
    logger.info("Sell %s %s amount: %f %f",\
                bb_exchange, pair, arb_amount, sell_price)
    # print("Sell", bb_exchange, pair, "amount", arb_amount, sell_price)
    logger.info("Buy %s %s amount: %f %f",\
                ba_exchange, pair, arb_amount, buy_price)
    # print("Buy", ba_exchange, pair, "amount", arb_amount, buy_price)

    # Launch orders
    for _ in range(3):
        try:
            sell_order = await exchanges[bb_exchange].\
        create_limit_sell_order(pair, arb_amount, sell_price)
            logger.info("Sell order: %s", sell_order)
        except Exception as mess:
            logger.warning(style.FAIL + "%s" + style.END, mess)
        else:
            break
    for _ in range(3):
        try:
            buy_order = await exchanges[ba_exchange].\
                create_limit_buy_order(pair, arb_amount, buy_price)
            logger.info("Buy order: %s", buy_order)
        except Exception as mess:
            logger.warning(style.FAIL + "%s" + style.END, mess)
        else:
            break



    # Check portfolio went up
    portfolio_up = False
    portfolio_counter = 0
    while portfolio_up == False and portfolio_counter < 2:
        # try getting balance up to 3 times
        for _ in range(3):
            try:
                bb_balance_after = await exchanges[bb_exchange].fetch_balance()
                ba_balance_after = await exchanges[ba_exchange].fetch_balance()
            except Exception as mess:
                logger.warning(style.FAIL + "%s" + style.END, mess)
            else:
                break
        try:
            portfolio_up, base_diff, quote_diff =\
                balance_check(pair.split("/")[0], pair.split("/")[1],\
                                 bb_balance, ba_balance,\
                                 bb_balance_after, ba_balance_after)
        except Exception as mess:
            logger.warning(style.FAIL + "%s" + style.END, mess)
        if not portfolio_up:
            time.sleep(5)
        portfolio_counter += 1

    logger.info("Trade gain (USD) %f, Percentage gain %f",\
                quote_diff * BTC_price['ask'],\
                100 * (quote_diff) / (arb_amount * best_bid))
    return(portfolio_up)


################################################################################
# Portfolio Balance
################################################################################

def balance_check(base, quote, bb_balance, ba_balance,\
                         bb_balance_after, ba_balance_after):

    base_diff = bb_balance_after[base]['total'] - bb_balance[base]['total'] +\
        ba_balance_after[base]['total'] - ba_balance[base]['total']
    logger.info("Portfolio change: %f %s", base_diff, base)
    quote_diff = bb_balance_after[quote]['total'] - bb_balance[quote]['total'] +\
        ba_balance_after[quote]['total'] - ba_balance[quote]['total']
    logger.info("Portfolio change: %f %s", quote_diff, quote)

    is_higher = base_diff>=-1e-5 and quote_diff>-1e-5

    return(is_higher, base_diff, quote_diff)

async def portfolio_balance(exchanges, arbitrableSymbols, inBTC=False):
    """ Returns the value of the portfolio in BTC for all currencies in
    arbitrable pairs
    """
    logger.info(style.OKBLUE + "Loading Portfolio Balances" + style.END)
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
        # try getting balance up to 3 times
        for _ in range(3):
            try:
                balance = await exchange.fetch_balance()
            except Exception as mess:
                logger.warning(style.FAIL + "%s" + style.END, mess)
            else:
                break

        logger.info(style.BOLD + "Exchange: %s" + style.END, id)
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
    logger.info(style.BOLD + "Currency totals" + style.END)
    try:
        for curr, total in portfolio.items():
            logger.info("%s %f", curr, total)
    except:
        pass

    if inBTC:
        for curr, total in portfolio.items():
            if curr == 'BTC':
                BTC_value += portfolio[curr] * 1.
            else:
                BTC_value += portfolio[curr] * CurrenciesBitcoinRate[curr]['ask']
        logger.info("Total portfolio balance: %f (BTC)", BTC_value)
        BTC_price = await ccxt.gemini().fetchTicker('BTC/USD')
        logger.info("Total portfolio balance: %f (USD)",\
                    BTC_value * BTC_price['ask'])

    return(portfolio)
