"""Per-job subprocess entry point. The process imports no Flask application."""
import itertools
import json
import os
from pathlib import Path
import sys
import resource
import time
from dotenv import load_dotenv
from qd_arbitrage import store
from qd_arbitrage.contracts import canonical, digest, validate
from .files import root, store_file, write_json_object, sha_file
from .data import import_dataset, replay_events, ns, utc


class Control:
    def __init__(self, job_id, token):
        self.job_id, self.token, self.last, self.stopped = job_id, token, 0, False
    def __call__(self):
        if time.monotonic()-self.last >= 1:
            row = store.job(self.job_id)
            self.stopped = not row or row['status'] != 'running'
            self.last = time.monotonic()
        return self.stopped
    def check(self):
        if self():
            raise RuntimeError('CANCEL_REQUESTED')


class EventSink:
    def __init__(self, job_id, token, run_id):
        self.job_id, self.token, self.run_id = job_id, token, run_id
        self.events, self.cycles, self.series_points, self.sequence = [], {}, [], 0
        self.artifacts = []
        self.last_flush = 0
        self.last_summary = {}
        self.event_hash = __import__('hashlib').sha256()
        self.dir = root() / 'runs' / run_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.market_file = None
        self.market_count = 0
        self.market_shard = 0

    def event(self, body):
        self.sequence += 1
        self.events.append((self.sequence, body))
        self.event_hash.update(canonical(body).encode())
        if len(self.events) >= 128:
            self.flush()

    def cycle(self, body):
        self.cycles[body['seq']] = json.loads(canonical(body))

    def series(self, points):
        self.series_points.extend(points)

    def market(self, body):
        if self.market_file is None:
            self.market_file = (self.dir / f'market-{self.market_shard}.partial').open('w')
        self.market_file.write(canonical(body)+'\n')
        self.market_count += 1
        if self.market_count >= 20000:
            self.close_market()

    def close_market(self):
        if self.market_file:
            name = self.market_file.name
            self.market_file.flush(); os.fsync(self.market_file.fileno()); self.market_file.close()
            self.artifacts.append({'kind': 'market', **store_file(name)})
            self.market_file = None; self.market_count = 0; self.market_shard += 1

    def checkpoint(self, summary, force=False):
        self.last_summary = summary
        if force or time.monotonic()-self.last_flush >= 2:
            self.flush()

    def flush(self):
        open_market = None
        if self.market_file:
            self.market_file.flush()
            os.fsync(self.market_file.fileno())
            open_market = {'path': str(Path(self.market_file.name).relative_to(root())),
                           'bytes': self.market_file.tell(), 'records': self.market_count}
        if self.events:
            artifact = write_json_object({'schemaVersion': 1, 'events': self.events})
            self.artifacts.append({'kind': 'engine_events', **artifact})
        summary = {**self.last_summary, 'eventCount': self.sequence, 'eventDigest': self.event_hash.hexdigest()}
        store.append_records(self.job_id, self.token, self.run_id, self.events, list(self.cycles.items()), summary, self.series_points)
        self.events.clear(); self.cycles.clear(); self.series_points.clear(); self.last_flush = time.monotonic()
        # Checkpoint manifest is mutable until COMPLETE; each underlying shard is immutable.
        temporary = self.dir / 'checkpoint.tmp'
        temporary.write_text(canonical({'artifacts': self.artifacts, 'openMarketShard': open_market, 'summary': summary}))
        temporary.replace(self.dir / 'checkpoint.json')

    def close(self, summary):
        self.close_market()
        summary = {**summary, 'eventCount': self.sequence, 'eventDigest': self.event_hash.hexdigest()}
        self.checkpoint(summary, True)
        manifest = write_json_object({'artifacts': self.artifacts, 'summary': self.last_summary, 'eventDigest': self.event_hash.hexdigest()})
        result = {**summary, 'artifactManifest': manifest}
        store.append_records(self.job_id, self.token, self.run_id, summary=result)
        (self.dir / 'COMPLETE').write_text(manifest['sha256'])
        return result


def backtest(config, manifest, sink, control, progress=lambda v: None):
    from .engine import build_engine, native_event
    engine, strategy, instruments = build_engine(config, manifest['rules'], sink, ns(utc(config['start'])), ns(utc(config['end'])), control)
    stream = (e for e in replay_events(manifest) if e['ts'] < ns(utc(config['end'])))
    count = 0
    try:
        while True:
            events = list(itertools.islice(stream, 1000))
            if not events:
                break
            if control():
                strategy.request_stop()
            engine.add_data([native_event(e, instruments) for e in events], sort=True)
            engine.run(streaming=True)
            engine.clear_data()
            strategy.advance()
            count += len(events)
            if strategy.error:
                raise RuntimeError(strategy.error)
            summary = {**strategy.report(), 'datasetId': manifest['id'], 'dataQuality': manifest['quality'], 'processedMessages': count, 'peakResidentBytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)}
            sink.series(strategy.series_snapshot(summary))
            sink.checkpoint(summary)
            if count % 10000 == 0:
                engine.cache.purge_closed_orders(strategy.clock.timestamp_ns(), buffer_secs=60)
                engine.cache.purge_closed_positions(strategy.clock.timestamp_ns(), buffer_secs=60)
                engine.cache.purge_account_events(strategy.clock.timestamp_ns(), lookback_secs=60)
            progress({'phase': 'replay', 'processedMessages': count, 'receivedTimeNs': events[-1]['ts']})
            if control() and (strategy.current is None or strategy.paused):
                break
        strategy.request_stop()
        strategy.advance()
        if strategy.current and not strategy.paused:
            strategy.paused = 'INTERVAL_ENDED_WITH_UNRESOLVED_ORDERS'
            strategy.current['state'] = 'manual'
            strategy.current['reason'] = strategy.paused
            sink.cycle(strategy.current)
        summary = {**strategy.report(as_of_ns=None if control() else ns(utc(config['end']))-1), 'datasetId': manifest['id'], 'dataQuality': manifest['quality'], 'processedMessages': count, 'peakResidentBytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)}
        sink.series(strategy.series_snapshot(summary))
        sink.checkpoint(summary)
        engine.end()
        return summary
    finally:
        # Engine is confined to this child process; an exception cannot reuse it.
        pass


def execute(job_id, token):
    row = store.job(job_id)
    control = Control(job_id, token)
    if row['kind'] == 'arb_import':
        manifest = import_dataset(row['request'], control.check, lambda p: store.progress(job_id, token, p))
        store.save_dataset(job_id, token, manifest)
        store.finish(job_id, token, 'succeeded', {'datasetId': manifest['id']})
        return
    config = validate(row['request'], row['request']['mode'])
    sink = EventSink(job_id, token, row['run_id'])
    if config['mode'] == 'paper':
        from .paper import run_paper
        summary = run_paper(config, sink, control)
    else:
        directory = root() / 'datasets' / config['datasetId']
        if not (directory / 'COMPLETE').is_file() or sha_file(directory / 'manifest.json') != (directory / 'COMPLETE').read_text():
            raise RuntimeError('DATASET_MANIFEST_INCOMPLETE_OR_CORRUPT')
        manifest = json.loads((directory / 'manifest.json').read_text())
        content = {k:v for k,v in manifest.items() if k!='id'}
        if digest(content) != config['datasetId']:
            raise RuntimeError('DATASET_ID_MISMATCH')
        summary = backtest(config, manifest, sink, control, lambda p: store.progress(job_id, token, p))
    result = sink.close(summary)
    store.finish(job_id, token, 'cancelled' if control() else 'succeeded', result)


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)
    job_id, token = sys.argv[1:3]
    try:
        execute(job_id, token)
    except BaseException as exc:
        # Provider requests are sanitized at their boundary; never emit full tracebacks.
        error = str(exc)[:1000]
        secret = os.getenv('CRYPTOHFTDATA_API_KEY')
        if secret:
            error = error.replace(secret, '[REDACTED]')
        try:
            store.finish(job_id, token, 'cancelled' if error=='CANCEL_REQUESTED' else 'failed', error=error)
        except RuntimeError:
            pass
        print(type(exc).__name__ + ': ' + error, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
