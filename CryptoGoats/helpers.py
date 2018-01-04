import asyncio
import ccxt


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


async def load_order_book(exchange, pair):
    """ Add order_book to Pandas dataframe
    pair = 'ETH/BTC'
    """
    orderbook = exchange.fetch_order_book(pair)

    row = [time.time(), exchange.id, pair, orderbook['timestamp'],\
     ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['bids'][:5])),\
     orderbook['bids'][0][0], # highest bid
     orderbook['bids'][0][1], # highest bid size
    ';'.join(map(lambda x: "@".join([str(x[1]), str(x[0])]), orderbook['asks'][:5])),\
    orderbook['asks'][0][0], # lowest ask
    orderbook['asks'][0][1], # lowest ask size
    1000]

    return(row)
