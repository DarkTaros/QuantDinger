"""Freeze public spot instrument rules into each immutable dataset."""
from datetime import datetime, timezone
from .files import client


def fetch_rules(pair):
    with client() as http:
        b = http.get('https://data-api.binance.vision/api/v3/exchangeInfo', params={'symbol': pair.replace('/', '')})
        if b.status_code != 200:
            raise RuntimeError('BINANCE_PUBLIC_INSTRUMENTS_UNAVAILABLE')
        info = b.json()['symbols'][0]
        filters = {f['filterType']: f for f in info['filters']}
        lot, price = filters['LOT_SIZE'], filters['PRICE_FILTER']
        notional = filters.get('NOTIONAL', filters.get('MIN_NOTIONAL'))
        o = http.get('https://www.okx.com/api/v5/public/instruments', params={'instType': 'SPOT', 'instId': pair.replace('/', '-')})
        if o.status_code != 200 or o.json().get('code') != '0' or not o.json().get('data'):
            raise RuntimeError('OKX_PUBLIC_INSTRUMENTS_UNAVAILABLE')
        okx = o.json()['data'][0]
    return {'asOf': datetime.now(timezone.utc).isoformat(),
            'assumption': 'Rules observed at import time; historical rule changes are not reconstructed.',
            'BINANCE': {'symbol': info['symbol'], 'step': lot['stepSize'], 'tick': price['tickSize'], 'minQty': lot['minQty'], 'minNotional': notional['minNotional']},
            'OKX': {'symbol': okx['instId'], 'step': okx['lotSz'], 'tick': okx['tickSz'], 'minQty': okx['minSz'], 'minNotional': '0'}}
