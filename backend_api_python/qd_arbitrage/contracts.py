"""Strict JSON boundary shared by Python 3.12 API and Python 3.14 workers."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

ENGINE = '2.0.0rc9'
TEMPLATE = 'cross_venue_taker_v1'
VENUES = ('BINANCE', 'OKX')
PAIRS = ('BTC/USDT', 'ETH/USDT', 'SOL/USDT')


def defaults(pair='BTC/USDT'):
    if pair not in PAIRS:
        raise ValueError('UNSUPPORTED_PAIR')
    return dict(schemaVersion=1, engineVersion=ENGINE, template=TEMPLATE,
                templateVersion=1, mode='backtest', pair=pair, seed=0,
                venues={v: {'quoteBalance': '10000', 'baseBalance': '0.1' if pair.startswith('BTC') else '1',
                            'takerFeeBps': '10', 'latencyMs': 50} for v in VENUES},
                strategy={'notional': '100', 'minEdgeBps': '5', 'maxQuoteAgeMs': 500,
                          'protectionBps': '5', 'timeoutMs': 2000, 'compensationBps': '20'},
                risk={'maxExposureQuote': '200', 'maxCycles': 100000})


def decimal_string(value, name, minimum=Decimal('0'), maximum=Decimal('1000000000')):
    if not isinstance(value, str) or len(value) > 48:
        raise ValueError(f'{name}: DECIMAL_STRING_REQUIRED')
    if not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value):
        raise ValueError(f'{name}: INVALID_DECIMAL')
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError(f'{name}: INVALID_DECIMAL') from None
    if not result.is_finite() or not minimum <= result <= maximum:
        raise ValueError(f'{name}: OUT_OF_RANGE')
    return format(result, 'f')


def utc(value):
    if not isinstance(value, str):
        raise ValueError('TIMEZONE_REQUIRED')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('INVALID_TIME') from None
    if result.tzinfo is None:
        raise ValueError('TIMEZONE_REQUIRED')
    return result.astimezone(timezone.utc)


def interval(payload):
    start, end = utc(payload.get('start')), utc(payload.get('end'))
    if not timedelta(0) < end - start <= timedelta(days=7):
        raise ValueError('INTERVAL_MUST_BE_POSITIVE_AND_AT_MOST_7_DAYS')
    return start, end


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(payload, mode='backtest'):
    if not isinstance(payload, dict):
        raise ValueError('OBJECT_REQUIRED')
    allowed = set(defaults()) | {'start', 'end', 'datasetId'}
    if set(payload) - allowed:
        raise ValueError('UNKNOWN_CONFIG_FIELDS')
    result = defaults(payload.get('pair', 'BTC/USDT'))
    for key, val in payload.items():
        if key in ('strategy', 'risk', 'venues'):
            if not isinstance(val, dict):
                raise ValueError(f'{key}: OBJECT_REQUIRED')
            if key == 'venues':
                if set(val) != set(VENUES):
                    raise ValueError('EXACTLY_TWO_VENUES_REQUIRED')
                for venue, params in val.items():
                    if not isinstance(params, dict) or set(params) - set(result[key][venue]):
                        raise ValueError('UNKNOWN_VENUE_FIELDS')
                    result[key][venue].update(params)
            else:
                if set(val) - set(result[key]):
                    raise ValueError('UNKNOWN_PARAMETER')
                result[key].update(val)
        else:
            result[key] = deepcopy(val)
    if type(result['schemaVersion']) is not int or type(result['templateVersion']) is not int:
        raise ValueError('UNSUPPORTED_CONTRACT_VERSION')
    if (result['schemaVersion'], result['engineVersion'], result['template'], result['templateVersion']) != (1, ENGINE, TEMPLATE, 1):
        raise ValueError('UNSUPPORTED_CONTRACT_VERSION')
    if result['mode'] != mode or mode not in ('backtest', 'paper'):
        raise ValueError('UNSUPPORTED_MODE')
    for params in [result['strategy'], result['risk'], *result['venues'].values()]:
        for key, value in params.items():
            if key.endswith('Ms') or key == 'maxCycles':
                if type(value) is not int or not 0 <= value <= (1000000 if key == 'maxCycles' else 60000):
                    raise ValueError(f'{key}: INVALID_INTEGER')
                if key != 'latencyMs' and value == 0:
                    raise ValueError(f'{key}: MUST_BE_POSITIVE')
            else:
                params[key] = decimal_string(value, key, maximum=Decimal('10000') if key.endswith('Bps') else Decimal('1000000000'))
    if Decimal(result['strategy']['notional']) <= 0:
        raise ValueError('NOTIONAL_MUST_BE_POSITIVE')
    if type(result['seed']) is not int or not 0 <= result['seed'] <= 2147483647:
        raise ValueError('INVALID_SEED')
    if mode == 'backtest':
        start, end = interval(result)
        result.update(start=start.isoformat(), end=end.isoformat())
        if not isinstance(result.get('datasetId'), str) or len(result['datasetId']) != 64 or any(c not in '0123456789abcdef' for c in result['datasetId']):
            raise ValueError('IMMUTABLE_DATASET_REQUIRED')
    elif set(payload) & {'datasetId', 'start', 'end'}:
        raise ValueError('PAPER_HAS_NO_HISTORICAL_INTERVAL')
    return result


def validate_import(payload):
    if not isinstance(payload, dict) or set(payload) - {'pair', 'start', 'end'}:
        raise ValueError('INVALID_IMPORT_CONFIG')
    if payload.get('pair') not in PAIRS:
        raise ValueError('UNSUPPORTED_PAIR')
    start, end = interval(payload)
    if end > datetime.now(timezone.utc) - timedelta(minutes=15):
        raise ValueError('HISTORICAL_FILES_NOT_YET_AVAILABLE')
    return {'pair': payload['pair'], 'start': start.isoformat(), 'end': end.isoformat()}
