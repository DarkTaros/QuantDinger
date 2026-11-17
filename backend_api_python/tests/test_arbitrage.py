"""Contract/auth regression tests; PostgreSQL integration is explicitly opt-in."""
import os
import uuid
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import pytest
from flask import Flask
from flask_smorest import Api
from qd_arbitrage import store
from qd_arbitrage.contracts import defaults, validate


@pytest.fixture
def client(monkeypatch):
    from app.routes.arbitrage import arbitrage_blp
    from app.utils import auth
    monkeypatch.setenv('ARBITRAGE_ENABLED','true')
    monkeypatch.setattr(auth,'verify_token',lambda token: {'user_id': int(token), '_verified_user_role': 'user', '_verified_username':'test'} if token in ('1','2') else None)
    app=Flask(__name__)
    app.config.update(TESTING=True,API_TITLE='Test',API_VERSION='1',OPENAPI_VERSION='3.0.3')
    Api(app).register_blueprint(arbitrage_blp,url_prefix='/api/arbitrage')
    return app.test_client()


def test_human_auth_and_module_switch(client,monkeypatch):
    assert client.get('/api/arbitrage/templates').status_code==401
    result=client.get('/api/arbitrage/templates',headers={'Authorization':'Bearer 1'})
    assert result.status_code==200 and result.json['code']==1
    assert result.json['data']['engineVersion']=='2.0.0rc9'
    monkeypatch.setenv('ARBITRAGE_ENABLED','false')
    assert client.get('/api/arbitrage/templates',headers={'Authorization':'Bearer 1'}).status_code==404


def test_templates_include_sol_spot_pair(client):
    result = client.get('/api/arbitrage/templates', headers={'Authorization': 'Bearer 1'})
    assert result.status_code == 200
    pairs = [item['pair'] for item in result.json['data']['items']]
    assert pairs == ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
    sol = next(item for item in result.json['data']['items'] if item['pair'] == 'SOL/USDT')
    assert sol['venues']['BINANCE']['baseBalance'] == '1'


def test_validation_before_queue_and_user_is_not_accepted_from_payload(client,monkeypatch):
    monkeypatch.setattr(store,'submit',lambda *a,**k: pytest.fail('Invalid input reached queue'))
    assert client.post('/api/arbitrage/backtests',json={'user_id':2},headers={'Authorization':'Bearer 1'}).status_code==400
    assert client.post('/api/arbitrage/paper-sessions',json={'mode':'live'},headers={'Authorization':'Bearer 1'}).status_code==400


def test_every_run_query_passes_authenticated_tenant(client,monkeypatch):
    calls=[]
    monkeypatch.setattr(store,'runs',lambda user,run=None: calls.append((user,run)) or None)
    for suffix in ('','/cycles','/events'):
        assert client.get('/api/arbitrage/runs/other'+suffix,headers={'Authorization':'Bearer 2'}).status_code==404
    assert calls==[(2,'other')]*3


@pytest.fixture
def isolated_db(monkeypatch):
    dsn=os.getenv('ARB_TEST_DATABASE_URL')
    if not dsn:
        pytest.skip('Set ARB_TEST_DATABASE_URL to run isolated PostgreSQL lifecycle tests')
    import psycopg2
    from psycopg2 import sql
    from pathlib import Path
    schema='test_arb_'+uuid.uuid4().hex
    admin=psycopg2.connect(dsn);admin.autocommit=True
    with admin.cursor() as cur:
        cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    # libpq keyword DSN preserves special characters without URL interpolation.
    params=psycopg2.extensions.parse_dsn(dsn);params['options']='-c search_path='+schema
    monkeypatch.setenv('DATABASE_URL',psycopg2.extensions.make_dsn(**params))
    try:
        with store.connection() as db,db.cursor() as cur:
            cur.execute('CREATE TABLE qd_agent_jobs (job_id TEXT PRIMARY KEY,user_id BIGINT,kind TEXT,status TEXT,request JSONB,idempotency_key TEXT,created_at TIMESTAMPTZ DEFAULT now(),started_at TIMESTAMPTZ,finished_at TIMESTAMPTZ,result JSONB,progress JSONB,error TEXT)')
            migration=(Path(__file__).resolve().parents[1]/'migrations/20261116_arbitrage.sql').read_text()
            cur.execute(migration)
            cur.execute((Path(__file__).resolve().parents[1]/'migrations/20261116_arbitrage_series.sql').read_text())
        yield
    finally:
        with admin.cursor() as cur:
            cur.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        admin.close()


def test_duplicate_claim_cancel_fencing_and_tenant_visibility(isolated_db):
    c={'pair':'BTC/USDT','start':'2026-11-01T00:00:00Z','end':'2026-11-02T00:00:00Z'}
    first=store.submit(1,'arb_import',c,'same')
    second=store.submit(1,'arb_import',c,'same')
    assert first['jobId']==second['jobId']
    with pytest.raises(ValueError,match='IDEMPOTENCY_CONFLICT'):
        store.submit(1,'arb_import',{**c,'pair':'ETH/USDT'},'same')
    assert store.job(first['jobId'],2) is None
    assert store.cancel(first['jobId'],2) is None
    with ThreadPoolExecutor(max_workers=2) as pool:
        tokens=list(pool.map(store.claim,[first['jobId']]*2))
    assert len([t for t in tokens if t])==1
    token=next(t for t in tokens if t)
    with pytest.raises(RuntimeError,match='LEASE'):
        store.finish(first['jobId'],'invalid-token','succeeded')
    store.cancel(first['jobId'],1)
    store.finish(first['jobId'],token,'succeeded',{'ok':True})
    assert store.job(first['jobId'],1)['status']=='cancelled'
    assert store.claim(first['jobId']) is None


def test_paper_exit_retains_checkpoint_and_never_restarts(isolated_db):
    c=defaults();c['mode']='paper'
    first=store.submit(1,'arb_paper',c)
    with pytest.raises(ValueError,match='CAPACITY'):
        store.submit(2,'arb_paper',c)
    token=store.claim(first['jobId'])
    store.append_records(first['jobId'],token,first['runId'],summary={'balances':{'OKX':{'USDT':'9990'}}})
    with store.connection() as db,db.cursor() as cur:
        cur.execute("UPDATE qd_arb_controls SET lease_until=now()-interval '1 second' WHERE job_id=%s",(first['jobId'],))
    store.reap_expired()
    result=store.runs(1,first['runId'])
    assert result['status']=='interrupted'
    assert result['summary']['balances']['OKX']['USDT']=='9990'
    assert store.claim(first['jobId']) is None
    assert store.runs(2,first['runId']) is None
    with pytest.raises(RuntimeError,match='LEASE'):
        store.append_records(first['jobId'],token,first['runId'],summary={})


def test_nanosecond_event_timestamps_survive_browser_json(client,monkeypatch):
    monkeypatch.setattr(store,'runs',lambda *args:{'id':'test'})
    monkeypatch.setattr(store,'records',lambda *args:{'items':[{'seq':1,'body':{'ts':1793491200000000001}}],'nextCursor':None})
    result=client.get('/api/arbitrage/runs/test/events',headers={'Authorization':'Bearer 1'})
    assert result.json['data']['items'][0]['body']['ts']=='1793491200000000001'
    assert result.json['data']['items'][0]['seq']==1


def test_series_route_is_tenant_scoped_and_normalizes_utc(client, monkeypatch):
    calls = []
    monkeypatch.setattr(store, 'runs', lambda user, run=None: calls.append(('run', user, run)) or {'id': run})
    monkeypatch.setattr(store, 'series', lambda user, run, metric, direction, after, limit: calls.append(('series', user, run, metric, direction, after, limit)) or {
        'items': [{'seq': 7, 'tsNs': 1793491200000000001, 'value': '1.25', 'unit': 'bps', 'direction': direction}],
        'nextCursor': None,
    })
    result = client.get('/api/arbitrage/runs/r-1/series?metric=spread&direction=buy_binance_sell_okx&resolution=7&cursor=3', headers={'Authorization': 'Bearer 2'})
    assert result.status_code == 200
    assert result.json['data']['timezone'] == 'UTC'
    assert result.json['data']['items'][0]['ts'] == '2026-11-01T00:00:00Z'
    assert calls[-1] == ('series', 2, 'r-1', 'spread', 'buy_binance_sell_okx', 3, 7)


def test_series_route_rejects_invalid_metric(client, monkeypatch):
    monkeypatch.setattr(store, 'runs', lambda *args: {'id': args[-1]})
    result = client.get('/api/arbitrage/runs/r-1/series?metric=not-a-metric', headers={'Authorization': 'Bearer 1'})
    assert result.status_code == 400


def test_fee_rates_return_account_bps_without_credentials(client, monkeypatch):
    import app.routes.arbitrage as route

    seen = []

    def owned_credential(user_id, exchange_id, requested):
        seen.append((user_id, exchange_id, requested))
        return {'id': 11 if exchange_id == 'binance' else 12, 'name': 'private'}

    monkeypatch.setattr(route, '_fee_credential_id', owned_credential)
    monkeypatch.setattr(route, 'build_exchange_config', lambda credential_id, user_id, overrides: {
        'credential_id': credential_id, 'user_id': user_id, **overrides,
    })
    monkeypatch.setattr(route, 'query_fee_rate', lambda config, pair, market_type: {
        'maker': 0.0008, 'taker': 0.001,
    })

    result = client.get(
        '/api/arbitrage/fee-rates?pair=BTC%2FUSDT&binanceCredentialId=11&okxCredentialId=12',
        headers={'Authorization': 'Bearer 2'},
    )
    assert result.status_code == 200
    data = result.json['data']
    assert data['venues']['BINANCE']['takerFeeBps'] == '10'
    assert data['venues']['OKX']['makerFeeBps'] == '8'
    assert seen == [(2, 'binance', '11'), (2, 'okx', '12')]
    assert 'secret' not in result.get_data(as_text=True).lower()


def test_fee_rates_do_not_accept_unknown_pair(client):
    result = client.get('/api/arbitrage/fee-rates?pair=BTC%2FUSD', headers={'Authorization': 'Bearer 1'})
    assert result.status_code == 400


def test_upload_route_queues_provider_named_files(client, monkeypatch, tmp_path):
    monkeypatch.setenv('ARB_DATA_DIR', str(tmp_path))
    captured = {}
    monkeypatch.setattr(store, 'submit', lambda user, kind, config, key: captured.update(user=user, kind=kind, config=config, key=key) or {'jobId': 'job-1', 'runId': None})
    response = client.post('/api/arbitrage/datasets/upload', data={
        'pair': 'BTC/USDT', 'start': '2026-11-01T00:00:00Z', 'end': '2026-11-01T01:00:00Z',
        'files': (BytesIO(b'PAR1'), 'binance_spot/2026-11-01/00/BTCUSDT_orderbook.parquet'),
    }, content_type='multipart/form-data', headers={'Authorization': 'Bearer 1'})
    assert response.status_code == 202
    assert captured['user'] == 1 and captured['kind'] == 'arb_import'
    assert 'binance_spot/2026-11-01/00/BTCUSDT_orderbook.parquet' in captured['config']['uploadedFiles']


def test_queued_cancel_timestamp_and_immutable_event_sequence(isolated_db):
    cfg=defaults();cfg['mode']='paper'
    first=store.submit(1,'arb_paper',cfg)
    store.cancel(first['jobId'],1)
    assert store.job(first['jobId'])['finished_at'] is not None
    second=store.submit(1,'arb_paper',cfg);token=store.claim(second['jobId'])
    store.append_records(second['jobId'],token,second['runId'],events=[(1,{'filled':'1'})])
    store.append_records(second['jobId'],token,second['runId'],events=[(1,{'filled':'1'})])
    with pytest.raises(RuntimeError,match='EVENT_SEQUENCE_CONFLICT'):
        store.append_records(second['jobId'],token,second['runId'],events=[(1,{'filled':'2'})])
    with pytest.raises(RuntimeError,match='RUN_JOB_MISMATCH'):
        store.append_records(second['jobId'],token,first['runId'])
    assert store.records(1,second['runId'],'events')['items'][0]['body']=={'filled':'1'}
