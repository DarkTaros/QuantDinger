"""Public feeds plus Sandbox only. No exchange execution factory is imported."""
import os
import signal
import asyncio
import time
from urllib.request import getproxies
from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.model import TraderId
from nautilus_trader.live import LiveNode
from nautilus_trader.live import DataClientConfig
from .ccxt_data import CCXTDataClientFactory
from nautilus_trader.adapters.sandbox import SandboxExecutionClientConfig, SandboxExecutionClientFactory
from qd_arbitrage.contracts import VENUES
from .engine import ArbitrageStrategy, instrument, venue_parameters, require_version
from .metadata import fetch_rules


class PaperStrategy(ArbitrageStrategy):
    def valid(self, venue, now=None):
        return self.health[venue]['valid'] and super().valid(venue, now)

    def report(self, as_of_ns=None):
        result = super().report(as_of_ns)
        result['feedProvider'] = 'ccxt.pro 4.5.92'
        result['feedHealth'] = {v:dict(state) for v,state in self.health.items()}
        result['assumptions'][1] = 'Local public-feed receive timestamps; real transport jitter is observed, not reconstructed.'
        result['assumptions'][-1] = 'Spot instrument rules observed at session startup.'
        return result

    def on_start(self):
        self.subscribe_socket_state()
        self.sockets = {}
        self.inject_disconnect = False
        self.started_at = time.monotonic()
        self.stop_started = None
        self.last_purge = time.monotonic()
        super().on_start()

    def on_socket_state(self, event):
        self.sockets[(str(event.client_id),event.endpoint)] = event.client_id
        super().on_socket_state(event)

    def on_book_deltas(self, deltas):
        super().on_book_deltas(deltas)

    def on_trade(self, trade):
        pass

    def on_tick(self, event):
        try:
            self.tick(event)
        except Exception as exc:
            self.error = type(exc).__name__ + ': ' + str(exc)
            self.paused = 'PAPER_CHECKPOINT_OR_CONTROL_FAILED'
            self.stopping = True
            self.node_handle.stop()

    def tick(self, event):
        if self.error:
            self.request_stop()
        if self.inject_disconnect:
            self.inject_disconnect = False
            self.sink.event({'type':'disconnect_injection','ts':self.clock.timestamp_ns()})
            for book in self.books.values():
                book['valid'] = False
            for factory in self.factories:
                client = factory.instance
                if client and client.exchange:
                    client.status['valid'] = False
                    client.create_task(client.exchange.close_ws_clients(), name='disconnect-injection')
        if self.current and any(not state['valid'] for state in self.health.values()):
            self.cancel_pending()
        super().on_tick(event)
        if time.monotonic()-self.last_purge >= 60:
            now = self.clock.timestamp_ns()
            self.cache.purge_closed_orders(now, buffer_secs=60)
            self.cache.purge_closed_positions(now, buffer_secs=60)
            self.cache.purge_account_events(now, lookback_secs=60)
            self.last_purge = time.monotonic()
        summary = self.report()
        self.sink.series(self.series_snapshot(summary))
        self.sink.checkpoint(summary)
        if self.stopping:
            self.stop_started = self.stop_started or time.monotonic()
            if self.current is None or self.paused or time.monotonic()-self.stop_started>10:
                if self.current and not self.paused:
                    self.paused = 'STOP_WITH_UNCONFIRMED_ORDERS'
                    self.current.update(state='manual',reason=self.paused)
                    self.sink.cycle(self.current)
                self.sink.checkpoint(self.report(), True)
                self.node_handle.stop()


def build_paper(config, sink, control):
    require_version()
    for key in list(os.environ):
        if key.startswith(('BINANCE_', 'OKX_')) and any(s in key for s in ('KEY','SECRET','PASSPHRASE')):
            os.environ.pop(key)
    rules = fetch_rules(config['pair'])
    instruments = {v:instrument(config['pair'],v,rules) for v in VENUES}
    builder = LiveNode.builder('quantdinger-arbitrage', TraderId('ARB-001'), Environment.SANDBOX)
    builder.with_logging(LoggerConfig(stdout_level=LogLevel.INFO))
    builder.with_reconciliation(False)
    builder.with_timeout_connection(60)
    health = {v: {'valid': False, 'error': None, 'messages': 0, 'disconnects': 0, 'lastReceiveNs': 0} for v in VENUES}
    factories = [CCXTDataClientFactory(v, config['pair'], instruments, health, sink) for v in VENUES]
    for venue, factory in zip(VENUES, factories):
        builder.add_data_client(venue, factory, DataClientConfig())
    for venue in VENUES:
        # Sandbox factory is the only allowable execution client.
        sandbox = SandboxExecutionClientConfig(**venue_parameters(config,venue))
        builder.add_simulated_exec_client(venue, SandboxExecutionClientFactory(), sandbox)
    node = builder.build()
    strategy = PaperStrategy(config,instruments,rules,sink,control=control)
    strategy.health = health
    strategy.factories = factories
    strategy.node_handle = node.handle()
    node.add_strategy(strategy)
    return node,strategy


def run_paper(config, sink, control):
    node,strategy = build_paper(config,sink,control)
    def stop(signum, frame):
        strategy.request_stop()
    def disconnect(signum, frame):
        strategy.inject_disconnect = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if hasattr(signal,'SIGUSR1'):
        signal.signal(signal.SIGUSR1, disconnect)
    try:
        async def run_node():
            await node.run_async()
        asyncio.run(run_node())
        if strategy.error:
            raise RuntimeError(strategy.error)
        return strategy.report()
    finally:
        node.dispose()
