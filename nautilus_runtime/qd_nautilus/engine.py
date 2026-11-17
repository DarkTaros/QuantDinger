"""Nautilus-owned L2 matching; Python controls paired IOC lifecycle and reporting."""
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from math import lcm
import importlib.metadata
import json
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.common import LoggerConfig, LogLevel
from nautilus_trader.execution import MakerTakerFeeModel, StaticLatencyModel
from nautilus_trader.model import (CurrencyPair, InstrumentId, Symbol, Currency, Price, Quantity, Money,
    Venue, OmsType, AccountType, BookType, OrderSide, TimeInForce, ClientOrderId,
    BookAction, BookOrder, OrderBookDelta, OrderBookDeltas, TradeTick, TradeId, AggressorSide)
from nautilus_trader.trading import Strategy, StrategyConfig
from qd_arbitrage.contracts import ENGINE, VENUES
from .data import ns
from .book import Levels

D = Decimal
BPS = D(10000)


def require_version():
    if importlib.metadata.version('nautilus_trader') != ENGINE:
        raise RuntimeError('EXACT_ENGINE_VERSION_REQUIRED')


def precision(value):
    return max(0, -D(value).normalize().as_tuple().exponent)


def round_step(value, step, up=False):
    step = D(step)
    return (value / step).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * step


def common_step(a, b):
    scale = 10 ** max(precision(a), precision(b))
    return D(lcm(int(D(a)*scale), int(D(b)*scale))) / scale


def instrument(pair, venue, rules):
    r = rules[venue]
    base = pair.split('/')[0]
    # Match official adapter identifiers, including OKX's spot symbol.
    iid = InstrumentId.from_str(r['symbol'] + '.' + venue)
    return CurrencyPair(iid, Symbol(r['symbol']), Currency.from_str(base), Currency.from_str('USDT'),
                        precision(r['tick']), precision(r['step']), Price.from_str(format(D(r['tick']).normalize(),'f')), Quantity.from_str(format(D(r['step']).normalize(),'f')),
                        0, 0, min_quantity=Quantity.from_str(f"{D(r['minQty']):.{precision(r['step'])}f}"),
                        min_notional=Money.from_str(r['minNotional']+' USDT') if D(r['minNotional']) else None)


def venue_parameters(config, venue):
    values = config['venues'][venue]
    base = config['pair'].split('/')[0]
    return dict(venue=Venue(venue), oms_type=OmsType.NETTING, account_type=AccountType.CASH,
                starting_balances=[Money.from_str(values['quoteBalance']+' USDT'), Money.from_str(values['baseBalance']+' '+base)],
                base_currency=None, book_type=BookType.L2_MBP, liquidity_consumption=True,
                bar_execution=False, trade_execution=True,
                fee_model=MakerTakerFeeModel(D(values['takerFeeBps'])/BPS, D(values['takerFeeBps'])/BPS),
                latency_model=StaticLatencyModel(base_latency_nanos=values['latencyMs'] * 1_000_000))


def native_event(event, instruments):
    ins = instruments[event['venue']]
    iid = ins.id
    def price_value(value):
        result = Price.from_str(f'{D(value):.{ins.price_precision}f}')
        if result.as_decimal() != D(value):
            raise ValueError('HISTORICAL_PRICE_PRECISION_MISMATCH')
        return result
    def size_value(value):
        result = Quantity.from_str(f'{D(value):.{ins.size_precision}f}')
        if result.as_decimal() != D(value):
            raise ValueError('HISTORICAL_SIZE_PRECISION_MISMATCH')
        return result
    if event['kind'] == 'trade':
        return TradeTick(iid, price_value(event['price']), size_value(event['quantity']),
                         AggressorSide.SELL if event['buyerMaker'] else AggressorSide.BUY,
                         TradeId(event['id']), event['eventNs'], event['ts'])
    deltas = []
    if event['snapshot']:
        deltas.append(OrderBookDelta(iid, BookAction.CLEAR,
            BookOrder(OrderSide.NO_ORDER_SIDE, price_value('0'), size_value('0'), 0),
            32, event['sequence'], event['eventNs'], event['ts']))
    for side, price, quantity in event['levels']:
        if side == 'noop':
            continue
        deltas.append(OrderBookDelta(iid, BookAction.DELETE if D(quantity)==0 else BookAction.UPDATE,
            BookOrder(OrderSide.BUY if side=='bid' else OrderSide.SELL, price_value(price), size_value(quantity), 0),
            32 if event['snapshot'] else 0, event['sequence'], event['eventNs'], event['ts']))
    if not deltas:
        raise ValueError('EMPTY_DELTA_MESSAGE')
    last = deltas[-1]
    deltas[-1] = OrderBookDelta(iid, last.action, last.order, last.flags | 128, last.sequence, last.ts_event, last.ts_init)
    return OrderBookDeltas(iid, deltas)


class MemorySink:
    """Small synthetic-test sink; production uses durable, flushing EventSink."""
    def __init__(self):
        self.events, self.cycles, self.series_points = [], {}, []
    def event(self, body):
        self.events.append(body)
    def cycle(self, body):
        self.cycles[body['seq']] = json.loads(json.dumps(body))
    def checkpoint(self, summary):
        self.summary = summary
    def series(self, points):
        self.series_points.extend(json.loads(json.dumps(points)))


class ArbitrageStrategy(Strategy):
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, StrategyConfig(order_id_tag='ARB', log_events=False, log_commands=False))

    def __init__(self, config, instruments, rules, sink, start_ns=0, end_ns=2**63-1, control=lambda: False):
        self.params, self.instruments, self.rules, self.sink = config, instruments, rules, sink
        self.start_ns, self.end_ns, self.control = start_ns, end_ns, control
        self.books = {v: {'bid': Levels(), 'ask': Levels(), 'ts': 0, 'valid': False} for v in VENUES}
        self.initial_marks = {}
        self.current = None
        self.cycle_count = 0
        self.order_seq = 0
        self.stopping = False
        self.paused = None
        self.realized = D(0)
        self.total_fees = D(0)
        self.compensation_cash = D(0)
        self.max_exposure = D(0)
        self.max_exposure_ns = 0
        self.exposure_since = None
        self.dust = D(0)
        self.completed = 0
        self.partial_cycles = 0
        self.common_step = common_step(rules['BINANCE']['step'], rules['OKX']['step'])
        self.processing = False
        self.error = None
        self.series_seq = 0
        self.equity_peak = None

    def on_start(self):
        for i in self.instruments.values():
            self.subscribe_book_deltas(i.id, BookType.L2_MBP, managed=True)
            self.subscribe_trades(i.id)
        self.clock.set_timer_ns('arb-control', 100_000_000, callback=self.on_tick)

    def on_tick(self, event):
        if self.control() or self.clock.timestamp_ns() >= self.end_ns:
            self.request_stop()
        self.advance()

    def on_socket_state(self, event):
        # Any connection transition invalidates both books until fresh snapshots.
        targets = [str(event.venue)] if event.venue is not None and str(event.venue) in self.books else list(self.books)
        for venue in targets:
            self.books[venue]['valid'] = False
        self.sink.event({'type': 'socket_state', 'ts': self.clock.timestamp_ns(), 'details': str(event)})

    def on_book_deltas(self, deltas):
        try:
            venue = str(deltas.instrument_id.venue)
            book = self.books[venue]
            for delta in deltas.deltas:
                if delta.action == BookAction.CLEAR:
                    book['bid'].clear(); book['ask'].clear(); book['valid'] = True
                else:
                    side = 'bid' if delta.order.side == OrderSide.BUY else 'ask'
                    p, q = delta.order.price.as_decimal(), delta.order.size.as_decimal()
                    if delta.action == BookAction.DELETE or q == 0:
                        book[side].pop(p, None)
                    else:
                        book[side][p] = q
            book['ts'] = deltas.ts_init
            if book['bid'] and book['ask'] and book['bid'].best(True) >= book['ask'].best():
                book['valid'] = False
            self.capture_initial_marks()
            self.advance()
            now = self.clock.timestamp_ns()
            if self.stopping or self.paused or self.current or now < self.start_ns or now >= self.end_ns:
                return
            if self.cycle_count >= self.params['risk']['maxCycles']:
                self.paused = 'MAX_CYCLES_REACHED'; return
            options = [self.opportunity('BINANCE', 'OKX'), self.opportunity('OKX', 'BINANCE')]
            options = [o for o in options if o is not None]
            if options:
                self.begin(max(options, key=lambda o: o['profit']))
        except Exception as exc:
            self.error = str(exc)
            self.paused = 'STRATEGY_ERROR'
            self.request_stop()
            raise

    def capture_initial_marks(self):
        if self.clock.timestamp_ns() >= self.start_ns and all(self.valid(v) for v in VENUES):
            for venue in VENUES:
                self.initial_marks.setdefault(venue, self.mark(venue))

    def valid(self, venue, now=None):
        b = self.books[venue]
        age = (self.clock.timestamp_ns() if now is None else now) - b['ts']
        return bool(b['valid'] and b['bid'] and b['ask'] and 0 <= age <= self.params['strategy']['maxQuoteAgeMs'] * 1_000_000)

    def mark(self, venue):
        b = self.books[venue]
        return (b['bid'].best(True)+b['ask'].best())/2

    def depth(self, venue, side, quantity, protection):
        book = self.books[venue]['ask' if side == 'buy' else 'bid']
        if not book:
            return None
        prices = book.ordered(descending=side=='sell')
        bound = book.best(high=side=='sell') * (1 + protection/BPS if side=='buy' else 1-protection/BPS)
        # Round towards the safe interior of the protection boundary.
        limit = round_step(bound, self.rules[venue]['tick'], up=side=='sell')
        remaining, value = quantity, D(0)
        for price in prices:
            if (side=='buy' and price>limit) or (side=='sell' and price<limit):
                break
            take = min(remaining, book[price]); value += take*price; remaining -= take
            if remaining == 0:
                return value / quantity, limit
        return None

    def free(self, venue, currency):
        account = self.cache.account_for_venue(Venue(venue))
        amount = account.balance_free(Currency.from_str(currency)) if account else None
        return amount.as_decimal() if amount is not None else D(0)

    def opportunity(self, buy, sell):
        if not all(self.valid(v) for v in VENUES):
            return None
        quantity = round_step(D(self.params['strategy']['notional']) / self.books[buy]['ask'].best(), self.common_step)
        if quantity <= 0:
            return None
        protection = D(self.params['strategy']['protectionBps'])
        a, b = self.depth(buy, 'buy', quantity, protection), self.depth(sell, 'sell', quantity, protection)
        if a is None or b is None:
            return None
        for venue, result in [(buy,a),(sell,b)]:
            if quantity < D(self.rules[venue]['minQty']) or quantity*result[0] < D(self.rules[venue]['minNotional']):
                return None
        buy_fee, sell_fee = [D(self.params['venues'][v]['takerFeeBps'])/BPS for v in (buy,sell)]
        if self.free(buy,'USDT') < quantity*a[1]*(1+buy_fee) or self.free(sell,self.params['pair'].split('/')[0]) < quantity:
            return None
        profit = quantity*(b[0]*(1-sell_fee)-a[0]*(1+buy_fee))
        if profit / (quantity*a[0])*BPS < D(self.params['strategy']['minEdgeBps']):
            return None
        if abs(self.dust)*max(a[0],b[0]) + quantity*max(a[1],b[1]) > D(self.params['risk']['maxExposureQuote']):
            return None
        return dict(buy=buy,sell=sell,quantity=quantity,profit=profit,buyLimit=a[1],sellLimit=b[1])

    def make_order(self, venue, side, quantity, price, compensation=False):
        ins = self.instruments[venue]
        self.order_seq += 1
        oid = ClientOrderId(f'ARB-{self.order_seq:010d}')
        order = self.order_factory.limit(ins.id, OrderSide.BUY if side=='buy' else OrderSide.SELL,
                Quantity.from_str(f'{quantity:.{ins.size_precision}f}'), Price.from_str(f'{price:.{ins.price_precision}f}'),
                time_in_force=TimeInForce.IOC, client_order_id=oid)
        leg = {'id': str(oid), 'venue': venue, 'side': side, 'planned': str(quantity), 'limit': str(price),
               'filled': '0', 'quote': '0', 'fee': '0', 'terminal': False, 'compensation': compensation}
        self.current['legs'].append(leg)
        return order

    def begin(self, signal):
        self.cycle_count += 1
        self.current = {'seq': self.cycle_count, 'state': 'waiting', 'startedNs': self.clock.timestamp_ns(),
                        'expectedProfit': str(signal['profit']), 'legs': [], 'compensationAttempted': False}
        self.processing = True
        try:
            orders = [self.make_order(signal['buy'], 'buy', signal['quantity'], signal['buyLimit']),
                      self.make_order(signal['sell'], 'sell', signal['quantity'], signal['sellLimit'])]
            self.current['state'] = 'submitted'
            self.sink.cycle(self.current)
            for order in orders:
                self.submit_order(order)
        finally:
            self.processing = False
        self.advance()

    def on_order_event(self, event):
        try:
            self.record_order_event(event)
        except Exception as exc:
            # The native callback can hold a cache borrow. Defer cancellation to
            # the next timer, and let the runner surface this as a failed run.
            self.error = type(exc).__name__ + ': ' + str(exc)
            self.paused = 'ORDER_EVENT_RECORDING_FAILED'
            self.stopping = True

    def record_order_event(self, event):
        body = event.to_dict()
        # UUID event identities are non-economic and intentionally excluded from replay digests.
        body.pop('event_id', None); body.pop('causation_id', None)
        self.sink.event({'type': type(event).__name__, 'ts': event.ts_event, 'details': body})
        if not self.current:
            return
        leg = next((l for l in self.current['legs'] if l['id'] == str(event.client_order_id)), None)
        if leg is None:
            return
        if type(event).__name__ == 'OrderFilled':
            qty, price = event.last_qty.as_decimal(), event.last_px.as_decimal()
            if str(event.commission.currency) != 'USDT':
                self.error = 'NON_QUOTE_FEE_NOT_SUPPORTED'; self.paused = self.error
                self.stopping = True; return
            fee = event.commission.as_decimal()
            leg['filled'] = str(D(leg['filled']) + qty)
            leg['quote'] = str(D(leg['quote']) + qty*price)
            leg['fee'] = str(D(leg['fee']) + fee)
            self.total_fees += fee
            self.current['state'] = 'partial'
        event_type = type(event).__name__
        if event_type in ('OrderDenied', 'OrderRejected', 'OrderCanceled', 'OrderExpired'):
            leg['terminal'] = True
        elif event_type == 'OrderFilled' and D(leg['filled']) >= D(leg['planned']):
            leg['terminal'] = True
        leg['status'] = event_type
        self.update_exposure(event.ts_event)
        self.sink.cycle(self.current)
        # Submission callbacks can hold a Rust cache mutable borrow. Advance from
        # the next book/timer callback, never re-enter execution from this callback.

    def imbalance(self):
        if not self.current:
            return D(0)
        return sum((D(l['filled'])*(1 if l['side']=='buy' else -1) for l in self.current['legs']), D(0))

    def update_exposure(self, now=None):
        exposure = abs(self.imbalance())
        now = self.clock.timestamp_ns() if now is None else now
        if exposure:
            if self.exposure_since is None:
                self.exposure_since = now
            marks = [self.mark(v) for v in VENUES if self.valid(v, now)]
            if marks:
                self.max_exposure = max(self.max_exposure, exposure*max(marks))
            self.max_exposure_ns = max(self.max_exposure_ns, now-self.exposure_since)
        else:
            if self.exposure_since is not None:
                self.max_exposure_ns = max(self.max_exposure_ns, now-self.exposure_since)
            self.exposure_since = None

    def request_stop(self):
        self.stopping = True
        if self.current:
            self.cancel_pending()

    def cancel_pending(self):
        if self.current.get('cancelRequested'):
            return
        self.current['cancelRequested'] = True
        # Set the marker before callbacks can re-enter advance().
        for leg in list(self.current['legs']):
            if not leg['terminal']:
                self.cancel_order(ClientOrderId(leg['id']))

    def advance(self):
        if not self.current or self.processing:
            return
        self.update_exposure()
        terminal = all(l['terminal'] for l in self.current['legs'])
        elapsed = self.clock.timestamp_ns() - self.current.get('attemptNs', self.current['startedNs'])
        if not terminal:
            if elapsed >= self.params['strategy']['timeoutMs'] * 1_000_000:
                self.cancel_pending()
                if elapsed >= (self.params['strategy']['timeoutMs']*2+1000)*1_000_000:
                    self.paused = 'ORDER_TERMINAL_UNCONFIRMED'
                    self.current['state'] = 'manual'
                    self.current['reason'] = self.paused
                    self.sink.cycle(self.current)
            return
        diff = self.imbalance()
        if diff == 0:
            self.complete('completed' if any(D(l['filled']) for l in self.current['legs']) else 'failed')
            return
        if self.current['compensationAttempted']:
            self.complete('manual', 'COMPENSATION_INCOMPLETE'); return
        primary = [l for l in self.current['legs'] if (l['side']=='buy') == (diff>0) and not l['compensation']]
        venue = primary[0]['venue']
        side = 'sell' if diff>0 else 'buy'
        if not self.valid(venue):
            self.complete('manual', 'COMPENSATION_MARKET_UNAVAILABLE'); return
        quantity = round_step(abs(diff), self.rules[venue]['step'])
        if quantity == 0 or quantity < D(self.rules[venue]['minQty']) or quantity*self.mark(venue) < D(self.rules[venue]['minNotional']):
            self.dust += diff
            self.complete('manual', 'DUST_REQUIRES_HANDLING'); return
        result = self.depth(venue, side, quantity, D(self.params['strategy']['compensationBps']))
        if result is None:
            self.complete('manual', 'COMPENSATION_INSUFFICIENT_DEPTH'); return
        self.current.update(state='compensating', compensationAttempted=True, attemptNs=self.clock.timestamp_ns(), cancelRequested=False)
        order = self.make_order(venue, side, quantity, result[1], True)
        self.sink.cycle(self.current)
        self.processing = True
        try:
            self.submit_order(order)
        finally:
            self.processing = False
        self.advance()

    def complete(self, state, reason=None):
        cycle = self.current
        cycle.update(state=state, reason=reason, finishedNs=self.clock.timestamp_ns(), exposure=str(self.imbalance()))
        cash = D(0)
        for leg in cycle['legs']:
            q, quote, fee = D(leg['filled']), D(leg['quote']), D(leg['fee'])
            leg['vwap'] = str(quote/q) if q else None
            flow = quote*(1 if leg['side']=='sell' else -1)-fee
            cash += flow
            if leg['compensation']:
                self.compensation_cash += flow
        cycle['netCashFlow'] = str(cash)
        cycle['realizedProfit'] = str(cash) if self.imbalance()==0 else None
        if self.imbalance()==0:
            self.realized += cash
        if any(0 < D(l['filled']) < D(l['planned']) for l in cycle['legs']):
            self.partial_cycles += 1
        if state == 'completed':
            self.completed += 1
        if state == 'manual':
            self.paused = reason
            # Retain unresolved cycle and its balance difference in every checkpoint.
            self.sink.cycle(cycle)
            self.processing = True
            return
        self.sink.cycle(cycle)
        self.current = None

    def report(self, as_of_ns=None):
        now = self.clock.timestamp_ns() if as_of_ns is None else as_of_ns
        self.update_exposure(now)
        balances, equity, initial_equity, hold = {}, D(0), D(0), D(0)
        valued = all(self.valid(v, now) and v in self.initial_marks for v in VENUES)
        base = self.params['pair'].split('/')[0]
        for venue in VENUES:
            account = self.cache.account_for_venue(Venue(venue))
            balances[venue] = {str(c): str(m.as_decimal()) for c,m in account.balances_total().items()} if account else {}
            if valued:
                mark = self.mark(venue)
                b = balances[venue]
                initial = self.params['venues'][venue]
                equity += D(b.get('USDT','0'))+D(b.get(base,'0'))*mark
                initial_equity += D(initial['quoteBalance'])+D(initial['baseBalance'])*self.initial_marks[venue]
                hold += D(initial['baseBalance'])*(mark-self.initial_marks[venue])
        current_exposure_quote = None
        if valued and self.imbalance():
            current_exposure_quote = abs(self.imbalance()) * max(self.mark(v) for v in VENUES)
        drawdown = None
        if valued:
            self.equity_peak = equity if self.equity_peak is None else max(self.equity_peak, equity)
            drawdown = equity - self.equity_peak
        return {'engineVersion': ENGINE, 'cycles': self.cycle_count, 'completedCycles': self.completed,
                'realizedProfit': str(self.realized), 'inventoryHoldingPnl': str(hold) if valued else None,
                'equity': str(equity) if valued else None, 'valuationAvailable': valued,
                'inventoryAndOpenExposureRevaluation': str(equity-initial_equity-hold-self.realized) if valued else None,
                'drawdown': str(drawdown) if drawdown is not None else None,
                'balances': balances, 'fees': str(self.total_fees), 'compensationCashFlow': str(self.compensation_cash),
                'exposureBase': str(self.imbalance()), 'exposureQuote': str(current_exposure_quote) if current_exposure_quote is not None else None,
                'dustBase': str(self.dust), 'maxExposureQuote': str(self.max_exposure),
                'maxExposureDurationNs': self.max_exposure_ns,
                'partialFillRate': str(D(self.partial_cycles)/self.cycle_count) if self.cycle_count else '0',
                'pausedReason': self.paused, 'asOfNs': now,
                'assumptions': ['Inbound order latency only; execution report latency is zero.',
                   'Provider receive timestamps represent its collection environment.',
                   'Quote-currency fees, no VIP discounts or rebates; no market impact.',
                   'L2 displayed liquidity consumption; deterministic equal-time stream ordering.',
                   self.rules.get('assumption', 'Synthetic instrument rules.')],
                'instrumentRules': self.rules, 'config': self.params}

    def series_snapshot(self, summary):
        """Return a compact, deterministic checkpoint for report charts."""
        ts = int(summary.get('asOfNs') or self.clock.timestamp_ns())
        self.series_seq += 1
        seq = self.series_seq
        points = []
        for metric, value, unit in (
            ('equity', summary.get('equity'), 'USDT'),
            ('exposure', summary.get('exposureQuote'), 'USDT'),
            ('fees', summary.get('fees'), 'USDT'),
            ('drawdown', summary.get('drawdown'), 'USDT'),
        ):
            if value is not None:
                points.append({'metric': metric, 'seq': seq, 'tsNs': ts, 'value': str(value), 'unit': unit})
        if all(self.valid(v, ts) for v in VENUES):
            base = self.books['BINANCE']['ask'].best()
            quote = self.books['OKX']['bid'].best()
            reverse_base = self.books['OKX']['ask'].best()
            reverse_quote = self.books['BINANCE']['bid'].best()
            points.extend([
                {'metric': 'spread', 'direction': 'buy_binance_sell_okx', 'seq': seq, 'tsNs': ts,
                 'value': str((quote - base) / base * BPS), 'unit': 'bps'},
                {'metric': 'spread', 'direction': 'buy_okx_sell_binance', 'seq': seq, 'tsNs': ts,
                 'value': str((reverse_quote - reverse_base) / reverse_base * BPS), 'unit': 'bps'},
            ])
        return points


def build_engine(config, rules, sink, start_ns=0, end_ns=2**63-1, control=lambda: False):
    require_version()
    instruments = {v: instrument(config['pair'], v, rules) for v in VENUES}
    engine = BacktestEngine(BacktestEngineConfig(run_analysis=False, logging=LoggerConfig(stdout_level=LogLevel.ERROR)))
    for venue in VENUES:
        engine.add_venue(**venue_parameters(config, venue), allow_cash_borrowing=False)
        engine.add_instrument(instruments[venue])
    strategy = ArbitrageStrategy(config, instruments, rules, sink, start_ns, end_ns, control)
    engine.add_strategy(strategy)
    return engine, strategy, instruments
