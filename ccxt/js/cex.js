"use strict";

//  ---------------------------------------------------------------------------

const Exchange = require ('./base/Exchange')
const { ExchangeError, AuthenticationError } = require ('./base/errors')

//  ---------------------------------------------------------------------------

module.exports = class cex extends Exchange {

    describe () {
        return this.deepExtend (super.describe (), {
            'id': 'cex',
            'name': 'CEX.IO',
            'countries': [ 'GB', 'EU', 'CY', 'RU' ],
            'rateLimit': 1500,
            'hasCORS': true,
            'hasFetchTickers': true,
            'hasFetchOHLCV': true,
            'hasFetchOpenOrders': true,
            'timeframes': {
                '1m': '1m',
            },
            'urls': {
                'logo': 'https://user-images.githubusercontent.com/1294454/27766442-8ddc33b0-5ed8-11e7-8b98-f786aef0f3c9.jpg',
                'api': 'https://cex.io/api',
                'www': 'https://cex.io',
                'doc': 'https://cex.io/cex-api',
            },
            'requiredCredentials': {
                'apiKey': true,
                'secret': true,
                'uid': true,
            },
            'api': {
                'public': {
                    'get': [
                        'currency_limits/',
                        'last_price/{pair}/',
                        'last_prices/{currencies}/',
                        'ohlcv/hd/{yyyymmdd}/{pair}',
                        'order_book/{pair}/',
                        'ticker/{pair}/',
                        'tickers/{currencies}/',
                        'trade_history/{pair}/',
                    ],
                    'post': [
                        'convert/{pair}',
                        'price_stats/{pair}',
                    ],
                },
                'private': {
                    'post': [
                        'active_orders_status/',
                        'archived_orders/{pair}/',
                        'balance/',
                        'cancel_order/',
                        'cancel_orders/{pair}/',
                        'cancel_replace_order/{pair}/',
                        'close_position/{pair}/',
                        'get_address/',
                        'get_myfee/',
                        'get_order/',
                        'get_order_tx/',
                        'open_orders/{pair}/',
                        'open_orders/',
                        'open_position/{pair}/',
                        'open_positions/{pair}/',
                        'place_order/{pair}/',
                    ],
                },
            },
            'fees': {
                'trading': {
                    'maker': 0,
                    'taker': 0.2 / 100,
                },
            },
        });
    }

    async fetchMarkets () {
        let markets = await this.publicGetCurrencyLimits ();
        let result = [];
        for (let p = 0; p < markets['data']['pairs'].length; p++) {
            let market = markets['data']['pairs'][p];
            let id = market['symbol1'] + '/' + market['symbol2'];
            let symbol = id;
            let [ base, quote ] = symbol.split ('/');
            result.push ({
                'id': id,
                'info': market,
                'symbol': symbol,
                'base': base,
                'quote': quote,
                'precision': {
                    'price': this.precisionFromString (market['minPrice']),
                    'amount': -1 * Math.log10 (market['minLotSize']),
                },
                'limits': {
                    'amount': {
                        'min': market['minLotSize'],
                        'max': market['maxLotSize'],
                    },
                    'price': {
                        'min': parseFloat (market['minPrice']),
                        'max': parseFloat (market['maxPrice']),
                    },
                    'cost': {
                        'min': market['minLotSizeS2'],
                        'max': undefined,
                    },
                },
            });
        }
        return result;
    }

    async fetchBalance (params = {}) {
        await this.loadMarkets ();
        let response = await this.privatePostBalance ();
        let result = { 'info': response };
        let ommited = [ 'username', 'timestamp' ];
        let balances = this.omit (response, ommited);
        let currencies = Object.keys (balances);
        for (let i = 0; i < currencies.length; i++) {
            let currency = currencies[i];
            if (currency in balances) {
                let account = {
                    'free': this.safeFloat (balances[currency], 'available', 0.0),
                    'used': this.safeFloat (balances[currency], 'orders', 0.0),
                    'total': 0.0,
                };
                account['total'] = this.sum (account['free'], account['used']);
                result[currency] = account;
            }
        }
        return this.parseBalance (result);
    }

    async fetchOrderBook (symbol, params = {}) {
        await this.loadMarkets ();
        let orderbook = await this.publicGetOrderBookPair (this.extend ({
            'pair': this.marketId (symbol),
        }, params));
        let timestamp = orderbook['timestamp'] * 1000;
        return this.parseOrderBook (orderbook, timestamp);
    }

    parseOHLCV (ohlcv, market = undefined, timeframe = '1m', since = undefined, limit = undefined) {
        return [
            ohlcv[0] * 1000,
            ohlcv[1],
            ohlcv[2],
            ohlcv[3],
            ohlcv[4],
            ohlcv[5],
        ];
    }

    async fetchOHLCV (symbol, timeframe = '1m', since = undefined, limit = undefined, params = {}) {
        await this.loadMarkets ();
        let market = this.market (symbol);
        if (!since)
            since = this.milliseconds () - 86400000; // yesterday
        let ymd = this.Ymd (since);
        ymd = ymd.split ('-');
        ymd = ymd.join ('');
        let request = {
            'pair': market['id'],
            'yyyymmdd': ymd,
        };
        let response = await this.publicGetOhlcvHdYyyymmddPair (this.extend (request, params));
        let key = 'data' + this.timeframes[timeframe];
        let ohlcvs = JSON.parse (response[key]);
        return this.parseOHLCVs (ohlcvs, market, timeframe, since, limit);
    }

    parseTicker (ticker, market = undefined) {
        let timestamp = undefined;
        let iso8601 = undefined;
        if ('timestamp' in ticker) {
            timestamp = parseInt (ticker['timestamp']) * 1000;
            iso8601 = this.iso8601 (timestamp);
        }
        let volume = this.safeFloat (ticker, 'volume');
        let high = this.safeFloat (ticker, 'high');
        let low = this.safeFloat (ticker, 'low');
        let bid = this.safeFloat (ticker, 'bid');
        let ask = this.safeFloat (ticker, 'ask');
        let last = this.safeFloat (ticker, 'last');
        let symbol = undefined;
        if (market)
            symbol = market['symbol'];
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': iso8601,
            'high': high,
            'low': low,
            'bid': bid,
            'ask': ask,
            'vwap': undefined,
            'open': undefined,
            'close': undefined,
            'first': undefined,
            'last': last,
            'change': undefined,
            'percentage': undefined,
            'average': undefined,
            'baseVolume': volume,
            'quoteVolume': undefined,
            'info': ticker,
        };
    }

    async fetchTickers (symbols = undefined, params = {}) {
        await this.loadMarkets ();
        let currencies = Object.keys (this.currencies);
        let response = await this.publicGetTickersCurrencies (this.extend ({
            'currencies': currencies.join ('/'),
        }, params));
        let tickers = response['data'];
        let result = {};
        for (let t = 0; t < tickers.length; t++) {
            let ticker = tickers[t];
            let symbol = ticker['pair'].replace (':', '/');
            let market = this.markets[symbol];
            result[symbol] = this.parseTicker (ticker, market);
        }
        return result;
    }

    async fetchTicker (symbol, params = {}) {
        await this.loadMarkets ();
        let market = this.market (symbol);
        let ticker = await this.publicGetTickerPair (this.extend ({
            'pair': market['id'],
        }, params));
        return this.parseTicker (ticker, market);
    }

    parseTrade (trade, market = undefined) {
        let timestamp = parseInt (trade['date']) * 1000;
        return {
            'info': trade,
            'id': trade['tid'],
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'symbol': market['symbol'],
            'type': undefined,
            'side': trade['type'],
            'price': parseFloat (trade['price']),
            'amount': parseFloat (trade['amount']),
        };
    }

    async fetchTrades (symbol, since = undefined, limit = undefined, params = {}) {
        await this.loadMarkets ();
        let market = this.market (symbol);
        let response = await this.publicGetTradeHistoryPair (this.extend ({
            'pair': market['id'],
        }, params));
        return this.parseTrades (response, market, since, limit);
    }

    async createOrder (symbol, type, side, amount, price = undefined, params = {}) {
        await this.loadMarkets ();
        let order = {
            'pair': this.marketId (symbol),
            'type': side,
            'amount': amount,
        };
        if (type == 'limit') {
            order['price'] = price;
        } else {
            // for market buy CEX.io requires the amount of quote currency to spend
            if (side == 'buy') {
                if (!price) {
                    throw new InvalidOrder ('For market buy orders ' + this.id + " requires the amount of quote currency to spend, to calculate proper costs call createOrder (symbol, 'market', 'buy', amount, price)");
                }
                order['amount'] = amount * price;
            }
            order['order_type'] = type;
        }
        let response = await this.privatePostPlaceOrderPair (this.extend (order, params));
        return {
            'info': response,
            'id': response['id'],
        };
    }

    async cancelOrder (id, symbol = undefined, params = {}) {
        await this.loadMarkets ();
        return await this.privatePostCancelOrder ({ 'id': id });
    }

    async fetchOrder (id, symbol = undefined, params = {}) {
        await this.loadMarkets ();
        return await this.privatePostGetOrder (this.extend ({
            'id': id.toString (),
        }, params));
    }

    parseOrder (order, market = undefined) {
        let timestamp = parseInt (order['time']);
        let symbol = undefined;
        if (!market) {
            let symbol = order['symbol1'] + '/' + order['symbol2'];
            if (symbol in this.markets)
                market = this.market (symbol);
        }
        let status = order['status'];
        if (status == 'cd') {
            status = 'canceled';
        } else if (status == 'c') {
            status = 'canceled';
        } else if (status == 'd') {
            status = 'closed';
        }
        let price = this.safeFloat (order, 'price');
        let amount = this.safeFloat (order, 'amount');
        let remaining = this.safeFloat (order, 'pending');
        if (!remaining)
            remaining = this.safeFloat (order, 'remains');
        let filled = amount - remaining;
        let fee = undefined;
        let cost = undefined;
        if (market) {
            symbol = market['symbol'];
            cost = this.safeFloat (order, 'ta:' + market['quote']);
            let baseFee = 'fa:' + market['base'];
            let quoteFee = 'fa:' + market['quote'];
            let feeRate = this.safeFloat (order, 'tradingFeeMaker');
            if (!feeRate)
                feeRate = this.safeFloat (order, 'tradingFeeTaker', feeRate);
            if (feeRate)
                feeRate /= 100.0; // convert to mathematically-correct percentage coefficients: 1.0 = 100%
            if (baseFee in order) {
                fee = {
                    'currency': market['base'],
                    'rate': feeRate,
                    'cost': this.safeFloat (order, baseFee),
                };
            } else if (quoteFee in order) {
                fee = {
                    'currency': market['quote'],
                    'rate': feeRate,
                    'cost': this.safeFloat (order, quoteFee),
                };
            }
        }
        if (!cost)
            cost = price * filled;
        return {
            'id': order['id'],
            'datetime': this.iso8601 (timestamp),
            'timestamp': timestamp,
            'status': status,
            'symbol': symbol,
            'type': undefined,
            'side': order['type'],
            'price': price,
            'cost': cost,
            'amount': amount,
            'filled': filled,
            'remaining': remaining,
            'trades': undefined,
            'fee': fee,
            'info': order,
        };
    }

    async fetchOpenOrders (symbol = undefined, since = undefined, limit = undefined, params = {}) {
        await this.loadMarkets ();
        let request = {};
        let method = 'privatePostOpenOrders';
        let market = undefined;
        if (symbol) {
            market = this.market (symbol);
            request['pair'] = market['id'];
            method += 'Pair';
        }
        let orders = await this[method] (this.extend (request, params));
        for (let i = 0; i < orders.length; i++) {
            orders[i] = this.extend (orders[i], { 'status': 'open' });
        }
        return this.parseOrders (orders, market, since, limit);
    }

    nonce () {
        return this.milliseconds ();
    }

    sign (path, api = 'public', method = 'GET', params = {}, headers = undefined, body = undefined) {
        let url = this.urls['api'] + '/' + this.implodeParams (path, params);
        let query = this.omit (params, this.extractParams (path));
        if (api == 'public') {
            if (Object.keys (query).length)
                url += '?' + this.urlencode (query);
        } else {
            this.checkRequiredCredentials ();
            let nonce = this.nonce ().toString ();
            let auth = nonce + this.uid + this.apiKey;
            let signature = this.hmac (this.encode (auth), this.encode (this.secret));
            body = this.urlencode (this.extend ({
                'key': this.apiKey,
                'signature': signature.toUpperCase (),
                'nonce': nonce,
            }, query));
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            };
        }
        return { 'url': url, 'method': method, 'body': body, 'headers': headers };
    }

    async request (path, api = 'public', method = 'GET', params = {}, headers = undefined, body = undefined) {
        let response = await this.fetch2 (path, api, method, params, headers, body);
        if (!response) {
            throw new ExchangeError (this.id + ' returned ' + this.json (response));
        } else if (response == true) {
            return response;
        } else if ('e' in response) {
            if ('ok' in response)
                if (response['ok'] == 'ok')
                    return response;
            throw new ExchangeError (this.id + ' ' + this.json (response));
        } else if ('error' in response) {
            if (response['error'])
                throw new ExchangeError (this.id + ' ' + this.json (response));
        }
        return response;
    }
}
