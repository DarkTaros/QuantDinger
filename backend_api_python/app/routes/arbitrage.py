"""Human-only, tenant-scoped arbitrage orchestration. No engine imports."""
import os
import re
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from functools import wraps
from flask import g, jsonify, request
from datetime import datetime, timezone
from app.openapi.blueprint import HumanBlueprint as Blueprint
from app.utils.auth import login_required
from app.utils.db import get_db_connection
from app.services.live_trading.factory import query_fee_rate
from app.services.quick_trade.credentials import build_exchange_config
from app.utils.logger import get_logger
from qd_arbitrage import store
from qd_arbitrage.schema import request_body, run_schema, IMPORT_SCHEMA, SUBMIT_DOC, PAGE_DOC
from qd_arbitrage.contracts import defaults, validate, validate_import, PAIRS, ENGINE

arbitrage_blp = Blueprint('arbitrage', __name__)
logger = get_logger(__name__)


def response(data, status=200):
    return jsonify(code=1, msg='success', data=json_safe(data)), status


def json_safe(value):
    # Browser JSON numbers cannot preserve nanosecond timestamps above 2**53.
    if type(value) is int and abs(value) > 2**53-1:
        return str(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def endpoint(fn):
    @wraps(fn)
    @login_required
    def wrapped(*args, **kwargs):
        if os.getenv('ARBITRAGE_ENABLED', 'false').lower() not in ('1','true','yes','on'):
            return jsonify(code=0, msg='arbitrage.disabled', data=None), 404
        try:
            return fn(*args, **kwargs)
        except ValueError as exc:
            reason = str(exc)
            return jsonify(code=0, msg='arbitrage.invalidRequest', data={'reason': reason}), 409 if reason in ('IDEMPOTENCY_CONFLICT','PAPER_CAPACITY_REACHED') else 400
    return wrapped


def missing():
    return jsonify(code=0, msg='arbitrage.notFound', data=None), 404


@arbitrage_blp.route('/templates', methods=['GET'])
@endpoint
def templates():
    return response({'engineVersion': ENGINE, 'items': [defaults(pair) for pair in PAIRS], 'maxDays': 7, 'paperCapacity': 1})


@arbitrage_blp.route('/instruments', methods=['GET'])
@endpoint
def instruments():
    return response({'items': [{'pair': p, 'venues': ['BINANCE','OKX'], 'market': 'spot'} for p in PAIRS]})


def _fee_rate_bps(value):
    """Convert an exchange's decimal fee rate to a JSON-safe bps string."""
    try:
        rendered = format(Decimal(str(value)) * Decimal('10000'), 'f')
        return rendered.rstrip('0').rstrip('.') or '0'
    except (InvalidOperation, TypeError, ValueError):
        return None


def _fee_credential_id(user_id, exchange_id, requested):
    """Resolve an owned credential without exposing whether another user owns an id."""
    params = [int(user_id), exchange_id]
    where = "user_id = %s AND exchange_id = %s"
    if requested not in (None, ''):
        try:
            requested_id = int(requested)
        except (TypeError, ValueError) as exc:
            raise ValueError('INVALID_CREDENTIAL_ID') from exc
        if requested_id <= 0:
            raise ValueError('INVALID_CREDENTIAL_ID')
        where += " AND id = %s"
        params.append(requested_id)
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            f"""SELECT id, name, api_key_hint
                FROM qd_exchange_credentials
                WHERE {where}
                ORDER BY updated_at DESC NULLS LAST, id DESC
                LIMIT 1""",
            tuple(params),
        )
        row = cur.fetchone()
        cur.close()
    return dict(row) if row else None


@arbitrage_blp.route('/fee-rates', methods=['GET'])
@arbitrage_blp.doc(parameters=[
    {'in': 'query', 'name': 'pair', 'required': False, 'schema': {'type': 'string', 'enum': PAIRS}},
    {'in': 'query', 'name': 'binanceCredentialId', 'required': False, 'schema': {'type': 'integer'}},
    {'in': 'query', 'name': 'okxCredentialId', 'required': False, 'schema': {'type': 'integer'}},
])
@endpoint
def fee_rates():
    """Return account-specific Binance and OKX spot fees for the current user.

    Credentials are decrypted only inside the backend and are never included in
    the response.  A venue that has no configured credential, or whose private
    API request fails, is reported as unavailable so callers can retain their
    manual fee assumption explicitly.
    """
    pair = str(request.args.get('pair') or 'BTC/USDT').strip().upper()
    if pair not in PAIRS:
        raise ValueError('UNSUPPORTED_PAIR')
    requested = {
        'BINANCE': request.args.get('binanceCredentialId'),
        'OKX': request.args.get('okxCredentialId'),
    }
    venues = {}
    fetched_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    for venue, exchange_id in (('BINANCE', 'binance'), ('OKX', 'okx')):
        item = {
            'available': False,
            'credentialId': None,
            'makerRate': None,
            'takerRate': None,
            'makerFeeBps': None,
            'takerFeeBps': None,
            'source': 'account_api',
            'fetchedAt': fetched_at,
            'reason': None,
        }
        try:
            row = _fee_credential_id(int(g.user_id), exchange_id, requested[venue])
            if not row:
                item['source'] = 'configured'
                item['reason'] = 'CREDENTIAL_NOT_CONFIGURED'
                logger.warning(
                    'Arbitrage fee rate skipped exchange=%s pair=%s reason=%s',
                    exchange_id, pair, item['reason'],
                )
                venues[venue] = item
                continue
            credential_id = int(row.get('id') or 0)
            config = build_exchange_config(credential_id, int(g.user_id), {'market_type': 'spot'})
            rate = query_fee_rate(config, pair, market_type='spot')
            item['credentialId'] = credential_id
            if not rate or rate.get('maker') is None or rate.get('taker') is None:
                item['reason'] = 'FEE_RATE_UNAVAILABLE'
                logger.warning(
                    'Arbitrage fee rate unavailable exchange=%s pair=%s credential_id=%s market_type=spot reason=%s',
                    exchange_id, pair, credential_id, item['reason'],
                )
            else:
                item['makerRate'] = str(rate['maker'])
                item['takerRate'] = str(rate['taker'])
                item['makerFeeBps'] = _fee_rate_bps(rate['maker'])
                item['takerFeeBps'] = _fee_rate_bps(rate['taker'])
                item['available'] = item['makerFeeBps'] is not None and item['takerFeeBps'] is not None
                if not item['available']:
                    item['reason'] = 'INVALID_FEE_RATE'
                    logger.warning(
                        'Arbitrage fee rate invalid exchange=%s pair=%s credential_id=%s market_type=spot reason=%s',
                        exchange_id, pair, credential_id, item['reason'],
                    )
                else:
                    logger.info(
                        'Arbitrage fee rate fetched exchange=%s pair=%s credential_id=%s market_type=spot '
                        'maker_rate=%s taker_rate=%s maker_bps=%s taker_bps=%s',
                        exchange_id, pair, credential_id, item['makerRate'], item['takerRate'],
                        item['makerFeeBps'], item['takerFeeBps'],
                    )
        except ValueError as exc:
            if str(exc) == 'INVALID_CREDENTIAL_ID':
                raise
            # A corrupt or no-longer-decryptable credential is unavailable for
            # this venue; do not expose its storage or provider error details.
            item['reason'] = 'FEE_RATE_UNAVAILABLE'
            logger.warning(
                'Arbitrage fee rate unavailable exchange=%s pair=%s reason=%s',
                exchange_id, pair, item['reason'],
            )
        except Exception:
            # Do not surface provider errors or decrypted credential contents.
            item['reason'] = 'FEE_RATE_UNAVAILABLE'
            logger.warning(
                'Arbitrage fee rate unavailable exchange=%s pair=%s reason=%s',
                exchange_id, pair, item['reason'],
            )
        venues[venue] = item
    return response({'pair': pair, 'marketType': 'spot', 'venues': venues, 'fetchedAt': fetched_at})


@arbitrage_blp.route('/datasets', methods=['GET'])
@endpoint
def datasets():
    return response({'items': store.datasets(int(g.user_id))})


@arbitrage_blp.route('/datasets/import', methods=['POST'])
@arbitrage_blp.doc(requestBody=request_body(IMPORT_SCHEMA), **SUBMIT_DOC)
@endpoint
def import_data():
    config = validate_import(request.get_json(silent=True))
    return response(store.submit(int(g.user_id), 'arb_import', config, request.headers.get('Idempotency-Key')), 202)


@arbitrage_blp.route('/datasets/upload', methods=['POST'])
@endpoint
def upload_data():
    """Queue a strict import using user-provided CryptoHFTData hour files.

    Filenames must be the provider object paths, for example
    ``binance_spot/2026-01-08/00/BTCUSDT_orderbook.parquet``.  Files are
    staged under the immutable arbitrage data root and consumed by the same
    normalizer as provider downloads.
    """
    base = validate_import({
        'pair': request.form.get('pair'),
        'start': request.form.get('start'),
        'end': request.form.get('end'),
    })
    files = request.files.getlist('files')
    if not files:
        raise ValueError('UPLOAD_FILES_REQUIRED')
    root = Path(os.getenv('ARB_DATA_DIR') or Path(__file__).resolve().parents[3] / 'data/arbitrage')
    upload_id = uuid.uuid4().hex
    upload_root = root / 'uploads' / upload_id
    upload_root.mkdir(parents=True, exist_ok=False)
    allowed = re.compile(r'^(binance_spot|okx_spot)/[0-9]{4}-[0-9]{2}-[0-9]{2}/[0-9]{2}/[A-Za-z0-9_-]+_(?:orderbook|trades)\.parquet(?:\.zst)?$')
    uploaded = {}
    try:
        for item in files:
            name = (item.filename or '').replace('\\', '/')
            if not allowed.fullmatch(name) or '..' in Path(name).parts:
                raise ValueError('INVALID_UPLOAD_FILENAME')
            destination = upload_root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            item.save(destination)
            source_name = name[:-4] if name.endswith('.zst') else name
            uploaded[source_name] = str(destination.relative_to(root))
    except Exception:
        import shutil
        shutil.rmtree(upload_root, ignore_errors=True)
        raise
    config = {**base, 'uploadedFiles': uploaded}
    return response(store.submit(int(g.user_id), 'arb_import', config, request.headers.get('Idempotency-Key')), 202)


@arbitrage_blp.route('/backtests', methods=['POST'])
@arbitrage_blp.doc(requestBody=request_body(run_schema('backtest')), **SUBMIT_DOC)
@endpoint
def backtests():
    config = validate(request.get_json(silent=True), 'backtest')
    return response(store.submit(int(g.user_id), 'arb_backtest', config, request.headers.get('Idempotency-Key')), 202)


@arbitrage_blp.route('/jobs/<job_id>', methods=['GET'])
@endpoint
def get_job(job_id):
    row = store.job(job_id, int(g.user_id))
    return response(row) if row else missing()


@arbitrage_blp.route('/jobs/<job_id>/cancel', methods=['POST'])
@endpoint
def cancel_job(job_id):
    result = store.cancel(job_id, int(g.user_id))
    return response(result) if result else missing()


@arbitrage_blp.route('/runs', methods=['GET'])
@endpoint
def runs():
    return response({'items': store.runs(int(g.user_id))})


@arbitrage_blp.route('/runs/<run_id>', methods=['GET'])
@endpoint
def run(run_id):
    result = store.runs(int(g.user_id), run_id)
    return response(result) if result else missing()


def page(run_id, kind):
    if not store.runs(int(g.user_id), run_id):
        return missing()
    after = int(request.args.get('cursor', '0'))
    limit = int(request.args.get('page_size', '100'))
    if after < 0 or not 1 <= limit <= 500:
        raise ValueError('INVALID_PAGINATION')
    return response(store.records(int(g.user_id), run_id, kind, after, limit))


@arbitrage_blp.route('/runs/<run_id>/cycles', methods=['GET'])
@arbitrage_blp.doc(**PAGE_DOC)
@endpoint
def cycles(run_id):
    return page(run_id, 'cycles')


@arbitrage_blp.route('/runs/<run_id>/events', methods=['GET'])
@arbitrage_blp.doc(**PAGE_DOC)
@endpoint
def events(run_id):
    return page(run_id, 'events')


@arbitrage_blp.route('/runs/<run_id>/series', methods=['GET'])
@arbitrage_blp.doc(parameters=[
    {'in': 'query', 'name': 'metric', 'required': True, 'schema': {'type': 'string', 'enum': ['equity', 'drawdown', 'spread', 'fees', 'exposure']}},
    {'in': 'query', 'name': 'direction', 'required': False, 'schema': {'type': 'string'}},
    {'in': 'query', 'name': 'resolution', 'required': False, 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 2000, 'default': 600}},
    {'in': 'query', 'name': 'cursor', 'required': False, 'schema': {'type': 'integer', 'minimum': 0, 'default': 0}},
])
@endpoint
def series(run_id):
    run = store.runs(int(g.user_id), run_id)
    if not run:
        return missing()
    metric = request.args.get('metric', '')
    direction = request.args.get('direction', '')
    resolution = int(request.args.get('resolution', '600'))
    cursor = int(request.args.get('cursor', '0'))
    result = store.series(int(g.user_id), run_id, metric, direction, cursor, resolution)
    items = []
    for item in result['items']:
        timestamp = datetime.fromtimestamp(item['tsNs'] / 1_000_000_000, tz=timezone.utc)
        items.append({'seq': item['seq'], 'ts': timestamp.isoformat().replace('+00:00', 'Z'),
                      'value': item['value'], 'unit': item['unit'], 'direction': item['direction']})
    return response({'runId': run_id, 'metric': metric, 'unit': items[0]['unit'] if items else '',
                     'timezone': 'UTC', 'items': items, 'nextCursor': result['nextCursor'],
                     'available': bool(items), 'reason': None if items else 'SERIES_NOT_AVAILABLE'})


@arbitrage_blp.route('/paper-sessions', methods=['POST'])
@arbitrage_blp.doc(requestBody=request_body(run_schema('paper')), **SUBMIT_DOC)
@endpoint
def paper():
    config = validate(request.get_json(silent=True), 'paper')
    return response(store.submit(int(g.user_id), 'arb_paper', config, request.headers.get('Idempotency-Key')), 202)


@arbitrage_blp.route('/paper-sessions/<run_id>/stop', methods=['POST'])
@endpoint
def stop_paper(run_id):
    result = store.runs(int(g.user_id), run_id)
    if not result or result['mode'] != 'paper':
        return missing()
    return response(store.cancel(result['job_id'], int(g.user_id)))
