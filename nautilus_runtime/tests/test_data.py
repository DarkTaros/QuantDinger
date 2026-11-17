from datetime import datetime, timezone
from decimal import Decimal
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from qd_nautilus.data import BookValidator,messages,ns
from qd_nautilus.files import retry_after,store_file,resolve_object
from qd_arbitrage.contracts import defaults,validate,validate_import

T=1793491200000000000

def head(kind='snapshot',seq=10,prev=None,first=None,t=T):
    return dict(event_type=kind,received_time=t,event_time=t//1000000,final_update_id=seq,prev_final_update_id=prev,first_update_id=first,last_update_id=seq)


def test_snapshot_delta_zero_delete_and_duplicate():
    b=BookValidator('OKX');levels=[('bid','100','2'),('ask','101','3')]
    assert b.accept(head(),levels)
    assert not b.accept(head(t=T+1),levels)
    assert b.duplicates==1
    b.accept(head('update',11,10,t=T+2),[('bid','100','0'),('bid','99','4')])
    assert Decimal('100') not in b.bids
    assert b.bids.best()==Decimal('99')
    b.accept(head(seq=20,t=T+3),[('noop','0','0')])
    assert not b.bids and not b.asks


def test_native_chain_and_clock_fail_closed():
    b=BookValidator('BINANCE')
    with pytest.raises(ValueError,match='INITIAL_SNAPSHOT'):
        b.accept(head('update',11,10,11),[])
    b.accept(head(),[('bid','100','1')])
    b.accept(head('update',13,None,11,t=T+1),[('ask','101','1')])
    with pytest.raises(ValueError,match='SEQUENCE_GAP'):
        b.accept(head('update',20,None,15,t=T+2),[])
    with pytest.raises(ValueError,match='RECEIVE_TIME'):
        b.accept(head('snapshot',21,t=T-1),[])


def test_message_group_crosses_arrow_batches(tmp_path):
    row={**head(), 'symbol':'BTC-USDT','side':'bid','price':'100','quantity':'1'}
    path=tmp_path/'book.parquet';pq.write_table(pa.Table.from_pylist([row]*9000),path,row_group_size=4000)
    groups=list(messages(path))
    assert len(groups)==1 and len(groups[0][1])==9000


def test_revised_source_has_new_content_hash_and_old_version_survives(tmp_path,monkeypatch):
    monkeypatch.setenv('ARB_DATA_DIR',str(tmp_path/'objects-root'))
    p=tmp_path/'source';p.write_bytes(b'PAR1old');a=store_file(p)
    p.write_bytes(b'PAR1new');b=store_file(p)
    assert a['sha256']!=b['sha256']
    assert resolve_object(a).read_bytes()==b'PAR1old'


def test_contract_exact_utc_interval_decimal_strings_and_max_seven_days():
    c=defaults();c.update(start='2026-11-01T08:30:00+08:00',end='2026-11-02T08:30:00+08:00',datasetId='a'*64)
    assert validate(c)['start']=='2026-11-01T00:30:00+00:00'
    c['venues']['OKX']['quoteBalance']=100
    with pytest.raises(ValueError,match='DECIMAL_STRING'):
        validate(c)
    with pytest.raises(ValueError,match='7_DAYS'):
        validate_import({'pair':'BTC/USDT','start':'2026-11-01T00:00:00Z','end':'2026-11-09T00:00:00Z'})
    with pytest.raises(ValueError,match='TIMEZONE'):
        validate_import({'pair':'BTC/USDT','start':'2026-11-01','end':'2026-11-02'})


def test_retry_after_and_ns():
    assert retry_after('2',0)==2
    assert ns(datetime(2026,11,1,tzinfo=timezone.utc))==T
    with pytest.raises(RuntimeError,match='RETRY_LATER'):
        retry_after('600',0)


@pytest.fixture
def hourly_source(tmp_path,monkeypatch):
    from qd_nautilus import data
    from test_engine import RULES
    monkeypatch.setenv('ARB_DATA_DIR',str(tmp_path/'store'))
    monkeypatch.setattr(data,'fetch_rules',lambda pair:RULES)
    sources={}
    for venue in ('BINANCE','OKX'):
        for hour in (0,1):
            ts=T+hour*3600*10**9
            h=head('snapshot' if hour==0 else 'update',10+hour,9+hour,10+hour,t=ts)
            for kind in ('orderbook','trades'):
                rows=[{**h,'side':side,'price':p,'quantity':'1'} for side,p in [('bid','60000'),('ask','60001')]] if kind=='orderbook' else [dict(received_time=ts,event_time=ts//10**6,price='60000',quantity='0.001',trade_id=hour,is_buyer_maker=False)]
                path=tmp_path/f'{venue}-{hour}-{kind}.parquet';pq.write_table(pa.Table.from_pylist(rows),path)
                obj=store_file(path)
                source=data.object_path(venue,'BTC/USDT',datetime(2026,11,1,hour,tzinfo=timezone.utc),kind)
                sources[source]={'source':source,'parquet':obj,'raw':obj,'converterVersion':'1'}
    def download(path,check):
        if path not in sources: raise FileNotFoundError('MISSING_HOUR: '+path)
        return sources[path]
    monkeypatch.setattr(data,'download',download)
    return data,sources,tmp_path


def test_cross_hour_import_atomic_replay_and_manifest(hourly_source):
    data,sources,path=hourly_source
    cfg=dict(pair='BTC/USDT',start='2026-11-01T00:30:00+00:00',end='2026-11-01T01:30:00+00:00')
    manifest=data.import_dataset(cfg)
    events=list(data.replay_events(manifest))
    assert len(events)==8
    assert [e['ts'] for e in events]==sorted(e['ts'] for e in events)
    assert events[0]['venue']=='BINANCE' and events[0]['kind']=='book'
    assert all(manifest['quality'][v]['strictValidation'] for v in ('BINANCE','OKX'))
    assert (path/'store/datasets'/manifest['id']/'COMPLETE').exists()


def test_requested_missing_hour_is_rejected(hourly_source):
    data,sources,_=hourly_source
    del sources['okx_spot/2026-11-01/01/BTC-USDT_orderbook.parquet']
    with pytest.raises(FileNotFoundError,match='MISSING_HOUR'):
        data.import_dataset(dict(pair='BTC/USDT',start='2026-11-01T00:30:00Z',end='2026-11-01T01:30:00Z'))


def test_cross_hour_sequence_gap_rejected_with_source(hourly_source):
    data,sources,path=hourly_source
    source='okx_spot/2026-11-01/01/BTC-USDT_orderbook.parquet'
    p=path/'bad.parquet';pq.write_table(pa.Table.from_pylist([{**head('update',12,8,t=T+3600*10**9),'side':'bid','price':'60000','quantity':'2'}]),p)
    sources[source]['parquet']=store_file(p)
    with pytest.raises(ValueError,match='BTC-USDT_orderbook.parquet: SEQUENCE_GAP'):
        data.import_dataset(dict(pair='BTC/USDT',start='2026-11-01T00:30:00Z',end='2026-11-01T01:30:00Z'))


def test_event_time_unit_error_rejected(hourly_source):
    data,sources,path=hourly_source
    source='okx_spot/2026-11-01/01/BTC-USDT_trades.parquet'
    p=path/'bad.parquet';pq.write_table(pa.Table.from_pylist([dict(received_time=T+3600*10**9,event_time=T//10**9,price='60000',quantity='1',trade_id=1,is_buyer_maker=False)]),p)
    sources[source]['parquet']=store_file(p)
    with pytest.raises(ValueError,match='EVENT_TIME_UNIT_OR_RANGE_INVALID'):
        data.import_dataset(dict(pair='BTC/USDT',start='2026-11-01T00:30:00Z',end='2026-11-01T01:30:00Z'))


def test_decimal_contract_rejects_exponent_and_nonfinite():
    from qd_arbitrage.contracts import decimal_string
    for value in ['1e-999999999','NaN','Infinity','-1','+1',' 1']:
        with pytest.raises(ValueError): decimal_string(value,'quantity')
