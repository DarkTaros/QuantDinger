"""Transport boundary checks use captured-shaped messages, never exchange orders."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace
import pytest
from qd_nautilus import ccxt_data as feeds
from qd_nautilus.data import BookValidator
from qd_nautilus.engine import instrument
from test_engine import RULES


def test_decimal_depth_and_public_only_endpoints():
    exchange=feeds.make_exchange('BINANCE',lambda *args:None,BookValidator('BINANCE'))
    assert exchange.urls['api']['public']=='https://data-api.binance.vision/api/v3'
    assert exchange.urls['api']['ws']['spot']=='wss://data-stream.binance.vision:443/ws'
    assert not exchange.apiKey and not exchange.secret
    assert exchange.parse_order_book_bid_ask(['60000.01','0.00001'])==[Decimal('60000.01'),Decimal('.00001')]


def test_binance_bootstrap_suppresses_partial_books_and_emits_final_snapshot(monkeypatch):
    emitted=[]
    exchange=feeds.make_exchange('BINANCE',lambda *args:emitted.append(args),BookValidator('BINANCE'))
    async def bootstrap(self,client,message,subscription):
        self.orderbooks['BTC/USDT']={'bids':[[Decimal('60000'),Decimal('1')]],'asks':[[Decimal('60001'),Decimal('1')]],'nonce':10,'timestamp':1000}
        self.handle_order_book_message(None,{'E':1001,'U':11,'u':11,'b':[],'a':[]},self.orderbooks['BTC/USDT'])
    monkeypatch.setattr(feeds.ccxt.binance,'fetch_order_book_snapshot',bootstrap)
    asyncio.run(exchange.fetch_order_book_snapshot(None,None,{'symbol':'BTC/USDT'}))
    assert len(emitted)==1 and emitted[0][0]=='snapshot' and emitted[0][1]==11
    assert not exchange.arb_syncing


def client_stub():
    native=[];market=[]
    client=SimpleNamespace(venue_name='OKX',validator=BookValidator('OKX'),
        instruments={v:instrument('BTC/USDT',v,RULES) for v in RULES},
        status={'valid':False,'messages':0,'lastReceiveNs':0},
        sink=SimpleNamespace(market=market.append),_handle_data=native.append)
    return client,native,market


def test_atomic_snapshot_update_delete_keepalive_and_gap():
    client,native,market=client_stub()
    emit=lambda *args:feeds.CCXTDataClient.emit_book(client,*args)
    emit('snapshot',10,-1,None,[('bid','60000','1'),('ask','60001','2')],1793491200000)
    emit('update',11,10,None,[('bid','60000','0'),('bid','59999','1')],1793491200001)
    assert len(native)==2 and len(native[1].deltas)==2
    assert market[1]['previousSequence']==10
    emit('update',11,11,None,[],1793491200002)
    assert len(native)==2
    with pytest.raises(ValueError,match='SEQUENCE_GAP'):
        emit('update',13,12,None,[],1793491200003)
    assert client.status['valid'] is False


def test_reconnect_retries_market_loading_failure(monkeypatch):
    async def exercise():
        attempts=[];events=[]
        class Exchange:
            async def load_markets(self):
                attempts.append('load')
                if len(attempts)==1: raise ConnectionError('temporary')
            async def close(self): pass
        async def books():
            client.closed=True
        async def trades():
            await asyncio.sleep(0)
        async def no_delay(_): pass
        client=SimpleNamespace(closed=False,exchange=None,venue_name='OKX',pair='BTC/USDT',
            validator=BookValidator('OKX'),status={'valid':False,'disconnects':0,'lastReceiveNs':0},
            sink=SimpleNamespace(event=events.append),emit_book=lambda *args:None,
            reset_book=lambda:None,books=books,trades=trades)
        monkeypatch.setattr(feeds,'make_exchange',lambda *args:Exchange())
        monkeypatch.setattr(feeds.asyncio,'sleep',no_delay)
        await feeds.CCXTDataClient.session(client)
        assert len(attempts)==2
        assert events[0]['reason']=='ConnectionError'
        assert not client.status['valid']
    asyncio.run(exercise())


def test_silent_watch_future_is_reconnected_by_watchdog(monkeypatch):
    async def exercise():
        events=[];clock=[100_000_000_000]
        class Exchange:
            async def close(self): client.closed=True
        async def pending(): await asyncio.Future()
        original_wait=asyncio.wait
        async def tick(tasks,**kwargs):
            clock[0]+=11_000_000_000
            return await original_wait(tasks,timeout=0)
        client=SimpleNamespace(closed=False,exchange=Exchange(),venue_name='BINANCE',
            status={'valid':True,'disconnects':0,'lastReceiveNs':0},
            sink=SimpleNamespace(event=events.append),books=pending,trades=pending)
        monkeypatch.setattr(feeds.time,'time_ns',lambda:clock[0])
        monkeypatch.setattr(feeds.asyncio,'wait',tick)
        await feeds.CCXTDataClient.session(client)
        assert events[0]['reason']=='TimeoutError'
        assert client.status['disconnects']==1 and not client.status['valid']
    asyncio.run(exercise())


def test_cancelled_transport_future_reconnects_without_cancelling_session():
    async def exercise():
        events=[]
        class Exchange:
            async def close(self): client.closed=True
        async def cancelled_watch(): raise asyncio.CancelledError()
        async def pending(): await asyncio.Future()
        client=SimpleNamespace(closed=False,exchange=Exchange(),venue_name='BINANCE',
            status={'valid':True,'disconnects':0,'lastReceiveNs':0},
            sink=SimpleNamespace(event=events.append),books=cancelled_watch,trades=pending)
        await feeds.CCXTDataClient.session(client)
        assert events[0]['reason']=='ConnectionError'
        assert client.status['disconnects']==1
    asyncio.run(exercise())
