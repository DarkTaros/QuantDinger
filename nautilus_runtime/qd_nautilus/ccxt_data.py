"""Public CCXT Pro messages routed through rc9's Python DataClient interface.

Raw message hooks preserve decimal strings and every accepted L2 batch. CCXT
owns transport/snapshot synchronization; our validator independently checks the
native sequence before publishing. No private API methods are called.
"""
import asyncio
from decimal import Decimal
import time
from urllib.request import getproxies
import os
import ccxt.pro as ccxt
from importlib.metadata import version
from nautilus_trader.live.clients import DataClient, DataClientFactory
from nautilus_trader.model import Venue
from .data import BookValidator
from .engine import native_event

CCXT_VERSION = '4.5.92'


class DecimalBookMixin:
    def parse_order_book_bid_ask(self, bidask, priceKey=0, amountKey=1, countOrIdKey=2):
        return [Decimal(bidask[priceKey]), Decimal(bidask[amountKey])]
    def handle_delta(self, bookside, delta):
        bookside.store(Decimal(delta[0]), Decimal(delta[1]))


class PublicBinance(DecimalBookMixin, ccxt.binance):
    async def fetch_order_book_snapshot(self, client, message, subscription):
        self.arb_syncing = True
        self.arb_reset()
        try:
            await super().fetch_order_book_snapshot(client, message, subscription)
            book = self.orderbooks.get(subscription['symbol'])
            if book is not None and book.get('nonce') is not None:
                levels = [('bid', str(p), str(q)) for p,q,*_ in book['bids']]
                levels += [('ask', str(p), str(q)) for p,q,*_ in book['asks']]
                self.arb_emit('snapshot', book['nonce'], None, None, levels,
                              book.get('timestamp') or time.time_ns()//1000000)
        finally:
            self.arb_syncing = False

    def handle_order_book_message(self, client, message, orderbook):
        # Buffered bootstrap deltas are not tradable until synchronization finishes.
        if getattr(self, 'arb_syncing', False):
            return super().handle_order_book_message(client, message, orderbook)
        if self.arb_validator.seq is None:
            levels = [('bid',str(p),str(q)) for p,q,*_ in orderbook['bids']]
            levels += [('ask',str(p),str(q)) for p,q,*_ in orderbook['asks']]
            self.arb_emit('snapshot',orderbook['nonce'],None,None,levels,message['E'])
        result = super().handle_order_book_message(client,message,orderbook)
        levels = [('bid',p,q) for p,q,*_ in message['b']]+[('ask',p,q) for p,q,*_ in message['a']]
        self.arb_emit('update',message['u'],None,message['U'],levels,message['E'])
        return result


class PublicOKX(DecimalBookMixin, ccxt.okx):
    def handle_order_book(self, client, message):
        result = super().handle_order_book(client,message)
        if message.get('arg',{}).get('channel') != 'books':
            return result
        for update in message.get('data',[]):
            levels = [('bid',p,q) for p,q,*_ in update['bids']]+[('ask',p,q) for p,q,*_ in update['asks']]
            self.arb_emit(message['action'],int(update['seqId']),int(update.get('prevSeqId',-1)),None,levels,int(update['ts']))
        return result


def make_exchange(venue, emit, validator, reset=lambda: None):
    if version('ccxt') != CCXT_VERSION:
        raise RuntimeError('EXACT_CCXT_VERSION_REQUIRED')
    proxy = os.getenv('ARB_HTTP_PROXY') or getproxies().get('https')
    cls = PublicBinance if venue=='BINANCE' else PublicOKX
    exchange = cls({'enableRateLimit':True,'newUpdates':True,
                    'httpsProxy':proxy,'wssProxy':proxy,
                    'options':{'defaultType':'spot','fetchMarkets':{'types':['spot']},'watchOrderBookLimit':5000}})
    exchange.number = str
    if venue=='BINANCE':
        # Binance's dedicated, credential-free public market-data endpoint.
        exchange.urls['api']['public'] = 'https://data-api.binance.vision/api/v3'
        exchange.urls['api']['ws']['spot'] = 'wss://data-stream.binance.vision:443/ws'
    exchange.arb_emit, exchange.arb_validator, exchange.arb_reset = emit, validator, reset
    return exchange


class CCXTDataClient(DataClient):
    def __init__(self, *, venue_name, pair, instruments, health, sink, **kwargs):
        super().__init__(venue=Venue(venue_name), **kwargs)
        self.venue_name, self.pair, self.instruments = venue_name,pair,instruments
        self.health, self.sink = health,sink
        self.validator = BookValidator(venue_name)
        self.exchange = None
        self.session_task = None
        self.closed = False
        self.trade_ids = set()
        from collections import deque
        self.trade_order = deque()
        self.status = health[venue_name]

    def emit_book(self, kind, sequence, previous, first, levels, event_ms):
        received = time.time_ns()
        head = dict(event_type=kind,final_update_id=sequence,last_update_id=sequence,
                    prev_final_update_id=previous,first_update_id=first,received_time=received,event_time=event_ms)
        try:
            accepted = self.validator.accept(head,levels)
        except ValueError:
            self.status.update(valid=False,error='L2_SEQUENCE_OR_BOOK_INVALID')
            raise
        if not accepted:
            return
        # Empty OKX keepalives confirm freshness without replenishing liquidity.
        if not levels and kind=='update':
            self.status['lastReceiveNs']=received
            return
        event = dict(kind='book',venue=self.venue_name,ts=received,eventNs=event_ms*1000000,
                     sequence=sequence,snapshot=kind=='snapshot',levels=levels)
        self.sink.market({**event, 'firstSequence': first, 'previousSequence': previous})
        native = native_event(event,self.instruments)
        self.status.update(valid=True,error=None,lastReceiveNs=received)
        self.status['messages'] += 1
        self._handle_data(native)

    def reset_book(self):
        self.status['valid'] = False
        self.validator = BookValidator(self.venue_name)
        if self.exchange:
            self.exchange.arb_validator = self.validator

    async def _connect(self):
        self.exchange = make_exchange(self.venue_name,self.emit_book,self.validator,self.reset_book)
        await self.exchange.load_markets()
        market=self.exchange.market(self.pair)
        if not market['spot']:
            raise RuntimeError('SPOT_MARKET_REQUIRED')
        self._handle_instrument(self.instruments[self.venue_name])

    async def _subscribe_book_deltas(self, command):
        if self.session_task is None:
            self.session_task = self.create_task(self.session(),name='ccxt-public-feeds')

    async def _subscribe_trades(self, command):
        pass

    async def _unsubscribe_book_deltas(self, command):
        if self.session_task:
            self.session_task.cancel()
            self.session_task=None

    async def _unsubscribe_trades(self, command):
        pass

    async def books(self):
        while not self.closed:
            params={'depth':'books'} if self.venue_name=='OKX' else {}
            await self.exchange.watch_order_book(self.pair,params=params)

    async def trades(self):
        while not self.closed:
            batch=await self.exchange.watch_trades(self.pair)
            for trade in batch:
                tid=str(trade['id'])
                if tid in self.trade_ids:
                    continue
                self.trade_ids.add(tid);self.trade_order.append(tid)
                if len(self.trade_order)>10000:
                    self.trade_ids.remove(self.trade_order.popleft())
                event=dict(kind='trade',venue=self.venue_name,ts=time.time_ns(),eventNs=trade['timestamp']*1000000,
                           price=str(trade['price']),quantity=str(trade['amount']),id=tid,buyerMaker=trade['side']=='sell')
                self.sink.market(event)
                self._handle_data(native_event(event,self.instruments))

    async def session(self):
        while not self.closed:
            tasks=[]
            try:
                if self.exchange is None:
                    self.reset_book()
                    self.exchange=make_exchange(self.venue_name,self.emit_book,self.validator,self.reset_book)
                    await self.exchange.load_markets()
                tasks=[asyncio.create_task(self.books()),asyncio.create_task(self.trades())]
                connected_at = time.time_ns()
                while not self.closed:
                    done,_=await asyncio.wait(tasks,timeout=1,return_when=asyncio.FIRST_COMPLETED)
                    if done:
                        for task in done:
                            if task.cancelled():
                                raise ConnectionError('PUBLIC_WATCH_CANCELLED')
                            task.result()
                        raise ConnectionError('PUBLIC_FEED_ENDED')
                    # Some transports close without rejecting watch futures. A
                    # receive watchdog forces a fresh connection and snapshot.
                    last = max(connected_at, self.status['lastReceiveNs'])
                    if time.time_ns()-last > 10_000_000_000:
                        raise TimeoutError('PUBLIC_BOOK_RECEIVE_TIMEOUT')
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.status.update(valid=False,error=type(exc).__name__)
                self.status['disconnects']+=1
                self.sink.event({'type':'feed_disconnected','venue':self.venue_name,'reason':type(exc).__name__,'ts':time.time_ns()})
            finally:
                self.status['valid']=False
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                if self.exchange:
                    await self.exchange.close()
                    self.exchange = None
            if not self.closed:
                await asyncio.sleep(1)

    async def _disconnect(self):
        self.closed=True
        self.status['valid']=False
        if self.session_task:
            self.session_task.cancel()
            await asyncio.gather(self.session_task,return_exceptions=True)
        if self.exchange:
            await self.exchange.close()


class CCXTDataClientFactory(DataClientFactory):
    def __init__(self,venue,pair,instruments,health,sink):
        self.args=dict(venue_name=venue,pair=pair,instruments=instruments,health=health,sink=sink)
        self.instance=None
    def create(self,**kwargs):
        self.instance=CCXTDataClient(**kwargs,**self.args)
        return self.instance
