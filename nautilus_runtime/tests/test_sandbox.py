"""Exercise the live Sandbox factory with synthetic public-data clients."""
import asyncio
import time
from decimal import Decimal
import pytest
from nautilus_trader.live.clients import DataClient, DataClientFactory
from nautilus_trader.model import Venue
from qd_arbitrage.contracts import defaults
from qd_nautilus import paper
from qd_nautilus.engine import MemorySink,native_event
from test_engine import RULES,book


class Sink(MemorySink):
    def checkpoint(self,summary,force=False): self.summary=summary


class SyntheticData(DataClient):
    async def _connect(self): self._handle_instrument(self.instruments[self.venue_name])
    async def _disconnect(self):
        if self.stream_task:
            self.stream_task.cancel()
            await asyncio.gather(self.stream_task,return_exceptions=True)
    async def _subscribe_trades(self,command): pass
    async def _unsubscribe_trades(self,command): pass
    async def _unsubscribe_book_deltas(self,command): pass
    async def _subscribe_book_deltas(self,command):
        self.stream_task=self.create_task(self.publish(),name='synthetic-books')
    async def publish(self):
        for step in range(15):
            event=book(self.venue_name,step*100,59999 if self.venue_name=='BINANCE' else 60300,60000 if self.venue_name=='BINANCE' else 60301)
            event['ts']=event['eventNs']=time.time_ns()
            self.health[self.venue_name]['valid']=True
            self._handle_data(native_event(event,self.instruments))
            await asyncio.sleep(.1)


class Factory(DataClientFactory):
    def __init__(self,venue,pair,instruments,health,sink):
        self.venue,self.instruments,self.health=venue,instruments,health
        self.instance=None
    def create(self,**kwargs):
        client=SyntheticData(venue=Venue(self.venue),**kwargs)
        client.venue_name,client.instruments,client.health=self.venue,self.instruments,self.health
        client.stream_task=None
        self.instance=client
        return client


@pytest.mark.parametrize('deny_sell', [False, True])
def test_live_sandbox_actual_ioc_fills_and_graceful_stop(monkeypatch, deny_sell):
    monkeypatch.setattr(paper,'CCXTDataClientFactory',Factory)
    monkeypatch.setattr(paper,'fetch_rules',lambda pair:RULES)
    config=defaults();config['mode']='paper';config['risk']['maxCycles']=1
    if deny_sell:
        config['venues']['OKX']['baseBalance']='0'
    sink=Sink();started=time.monotonic()
    node,strategy=paper.build_paper(config,sink,lambda:time.monotonic()-started>1.0)
    if deny_sell:
        free=strategy.free
        strategy.free=lambda venue,currency: Decimal('1') if venue=='OKX' and currency=='BTC' else free(venue,currency)
    async def exercise(): await asyncio.wait_for(node.run_async(),timeout=15)
    try:
        asyncio.run(exercise())
        assert strategy.error is None
        result=strategy.report()
        assert result['completedCycles']==1
        if deny_sell:
            assert sink.cycles[1]['compensationAttempted']
            assert Decimal(result['balances']['OKX']['BTC'])==0
            assert Decimal(result['balances']['BINANCE']['BTC'])==Decimal('.1')
            assert any(event['type'] in ('OrderDenied','OrderRejected') for event in sink.events)
        else:
            assert Decimal(result['realizedProfit'])==Decimal('0.298302')
            assert Decimal(result['balances']['BINANCE']['BTC'])==Decimal('.10166')
            assert Decimal(result['balances']['OKX']['BTC'])==Decimal('.09834')
        assert result['exposureBase']=='0'
        assert strategy.stopping
    finally:
        node.dispose()
