"""Streaming CryptoHFTData message reconstruction and immutable normalization."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
import heapq
import itertools
import json
import os
import tempfile
import threading
import pyarrow.parquet as pq
import zstandard
from qd_arbitrage.contracts import canonical, digest, utc, VENUES
from .files import download, resolve_object, root, store_file, write_json_object, CONVERTER_VERSION, quota_check
from .metadata import fetch_rules
from .book import Levels

MESSAGE_FIELDS = ('received_time', 'event_time', 'event_type', 'first_update_id',
                  'final_update_id', 'prev_final_update_id', 'last_update_id')


def ns(dt):
    epoch = utc('1970-01-01T00:00:00Z')
    d = dt - epoch
    return (d.days * 86400 + d.seconds) * 1_000_000_000 + d.microseconds * 1000


def rows(path):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=8192):
        yield from batch.to_pylist()


def messages(path):
    # groupby spans Arrow record batches and retains published row order.
    for key, group in itertools.groupby(rows(path), lambda r: tuple(r.get(k) for k in MESSAGE_FIELDS)):
        result = list(itertools.islice(group, 100001))
        if len(result) > 100000:
            raise ValueError('MESSAGE_TOO_LARGE')
        yield dict(zip(MESSAGE_FIELDS, key)), result


class BookValidator:
    def __init__(self, venue):
        self.venue, self.seq, self.time = venue, None, 0
        self.last_snapshot = None
        self.bids, self.asks = Levels(), Levels()
        self.duplicates = 0
        self.initialized = False

    def accept(self, header, levels):
        snapshot = header['event_type'] == 'snapshot'
        if header['event_type'] not in ('snapshot', 'update'):
            raise ValueError('UNKNOWN_BOOK_EVENT')
        end = header.get('final_update_id')
        end = header.get('last_update_id') if end is None else end
        if end is None:
            raise ValueError('NATIVE_SEQUENCE_REQUIRED')
        received = header['received_time']
        if type(received) is not int or received < 10**17 or received < self.time:
            raise ValueError(f'RECEIVE_TIME_REVERSED_OR_NOT_NS: {received}')
        fingerprint = digest([end, levels]) if snapshot else None
        if snapshot and fingerprint == self.last_snapshot:
            self.duplicates += 1
            self.time = received
            return False
        if not snapshot:
            if self.seq is None:
                raise ValueError('INITIAL_SNAPSHOT_REQUIRED')
            if self.venue == 'BINANCE':
                first = header.get('first_update_id')
                if first is None or not first <= self.seq + 1 <= end:
                    raise ValueError(f'SEQUENCE_GAP: previous={self.seq}, first={first}, final={end}')
            elif header.get('prev_final_update_id') != self.seq:
                raise ValueError(f'SEQUENCE_GAP: previous={self.seq}, prev={header.get("prev_final_update_id")}')
        if snapshot:
            self.bids.clear(); self.asks.clear()
        for side, price, quantity in levels:
            if side == 'noop' and snapshot:
                continue
            if side not in ('bid', 'ask'):
                raise ValueError('INVALID_BOOK_SIDE')
            p, q = Decimal(price), Decimal(quantity)
            if not p.is_finite() or not q.is_finite() or p <= 0 or q < 0:
                raise ValueError('INVALID_BOOK_LEVEL')
            book = self.bids if side == 'bid' else self.asks
            if q == 0:
                book.pop(p, None)
            else:
                book[p] = q
        if self.bids and self.asks and self.bids.best(True) >= self.asks.best():
            raise ValueError('CROSSED_BOOK')
        self.seq, self.time, self.initialized = end, received, True
        self.last_snapshot = fingerprint
        return True


def normalized(file, venue, kind, validator=None):
    path = resolve_object(file['parquet'])
    if kind == 'orderbook':
        for head, batch in messages(path):
            levels = [(r['side'], r['price'], r['quantity']) for r in batch]
            if validator.accept(head, levels):
                yield {'kind': 'book', 'venue': venue, 'ts': head['received_time'],
                       'eventNs': head['event_time'] * 1_000_000, 'sequence': validator.seq,
                       'snapshot': head['event_type'] == 'snapshot', 'levels': levels}
    else:
        last_time = 0
        for row in rows(path):
            t = row['received_time']
            if t < last_time or t < 10**17:
                raise ValueError('TRADE_RECEIVE_TIME_REVERSED_OR_NOT_NS')
            last_time = t
            yield {'kind': 'trade', 'venue': venue, 'ts': t, 'eventNs': row['event_time'] * 1_000_000,
                   'price': row['price'], 'quantity': row['quantity'], 'id': str(row['trade_id']),
                   'buyerMaker': row['is_buyer_maker']}


def object_path(venue, pair, hour, kind):
    symbol = pair.replace('/', '' if venue == 'BINANCE' else '-')
    return f'{venue.lower()}_spot/{hour:%Y-%m-%d/%H}/{symbol}_{kind}.parquet'


def local_upload(relative_path, source_name):
    """Install an uploaded provider file as an immutable object.

    The API only stores a relative path beneath the shared arbitrage root;
    resolving it here keeps filesystem access inside that root and reuses the
    exact parquet/zstd validation used by provider downloads.
    """
    candidate = (root() / relative_path).resolve()
    if root().resolve() not in candidate.parents or not candidate.is_file():
        raise FileNotFoundError(f'UPLOADED_FILE_NOT_FOUND: {source_name}')
    raw = store_file(str(candidate))
    path = resolve_object(raw)
    with path.open('rb') as src:
        magic = src.read(4)
    if magic == b'PAR1':
        parquet = raw
    elif magic == b'\x28\xb5\x2f\xfd':
        fd, name = tempfile.mkstemp(dir=root()); os.close(fd)
        try:
            with path.open('rb') as src, zstandard.ZstdDecompressor().stream_reader(src) as reader, open(name, 'wb') as out:
                while chunk := reader.read(1024 * 1024):
                    quota_check(len(chunk)); out.write(chunk)
            with open(name, 'rb') as test:
                if test.read(4) != b'PAR1':
                    raise RuntimeError('INVALID_PARQUET_HEADER')
            parquet = store_file(name)
        finally:
            from pathlib import Path
            Path(name).unlink(missing_ok=True)
    else:
        raise RuntimeError(f'UNEXPECTED_FILE_FORMAT: {source_name}')
    return {'source': f'upload:{source_name}', 'downloadedAt': utc('1970-01-01T00:00:00Z').isoformat(),
            'raw': raw, 'parquet': parquet, 'converterVersion': CONVERTER_VERSION}


def import_dataset(config, check=lambda: None, progress=lambda value: None):
    start, end = utc(config['start']), utc(config['end'])
    hour = start.replace(minute=0, second=0, microsecond=0)
    hours = []
    while hour < end:
        hours.append(hour); hour += timedelta(hours=1)
    rules = fetch_rules(config['pair'])
    files = {}
    uploaded = config.get('uploadedFiles') or {}
    tasks = [(v, h, k) for h in hours for v in VENUES for k in ('orderbook', 'trades')]
    aborted = threading.Event()
    def download_check():
        if aborted.is_set():
            raise RuntimeError('IMPORT_DOWNLOAD_ABORTED')
        check()
    def fetch(task):
        v, h, k = task
        download_check()
        source = object_path(v, config['pair'], h, k)
        if source in uploaded:
            return task, local_upload(uploaded[source], source)
        return task, download(source, download_check)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            for n, (key, file) in enumerate(pool.map(fetch, tasks, buffersize=2)):
                files[key] = file
                progress({'phase': 'download', 'completed': n+1, 'total': len(tasks)})
        except BaseException:
            aborted.set()
            pool.shutdown(wait=True, cancel_futures=True)
            raise
    # Find a snapshot at/before start, never silently skip the requested prefix.
    for venue in VENUES:
        warm = []
        for distance in range(25):
            candidate = hours[0] - timedelta(hours=distance)
            key = (venue, candidate, 'orderbook')
            if key not in files:
                source = object_path(venue, config['pair'], candidate, 'orderbook')
                if source in uploaded:
                    files[key] = local_upload(uploaded[source], source)
                else:
                    files[key] = download(source, check)
            warm.insert(0, candidate)
            if any(h['event_type'] == 'snapshot' and h['received_time'] <= ns(start)
                   for h, _ in messages(resolve_object(files[key]['parquet']))):
                break
        else:
            raise ValueError(f'NO_RELIABLE_SNAPSHOT_WITHIN_24_HOURS: {venue}')
    shards, quality = [], {}
    for venue in VENUES:
        validator = BookValidator(venue)
        initialized = False
        last_trade_ts = 0
        venue_files = sorted((k for k in files if k[0] == venue), key=lambda k: (k[1], k[2]))
        for file_index, key in enumerate(venue_files):
            _, hour, kind = key
            check()
            file = files[key]
            # The warmup prefix before its first snapshot cannot be reconstructed.
            if kind == 'orderbook' and not initialized:
                source = resolve_object(file['parquet'])
                def first_snapshot_stream():
                    nonlocal initialized
                    for head, batch in messages(source):
                        if not initialized and head['event_type'] != 'snapshot':
                            continue
                        initialized = True
                        levels = [(r['side'], r['price'], r['quantity']) for r in batch]
                        if validator.accept(head, levels):
                            yield {'kind': 'book', 'venue': venue, 'ts': head['received_time'], 'eventNs': head['event_time']*1_000_000,
                                   'sequence': validator.seq, 'snapshot': head['event_type']=='snapshot', 'levels': levels}
                stream = first_snapshot_stream()
            else:
                stream = normalized(file, venue, kind, validator)
            fd, name = tempfile.mkstemp(dir=root()); os.close(fd)
            count, first, last = 0, None, None
            try:
                with open(name, 'wb') as output, zstandard.ZstdCompressor(level=3).stream_writer(output) as writer:
                    for event in stream:
                        if kind == 'trades':
                            if event['ts'] < last_trade_ts:
                                raise ValueError('CROSS_FILE_TRADE_TIME_REVERSED')
                            last_trade_ts = event['ts']
                        if not ns(hour) <= event['ts'] < ns(hour+timedelta(hours=1)):
                            raise ValueError('RECEIVE_TIME_OUTSIDE_SOURCE_HOUR')
                        if event['ts'] >= ns(end):
                            break
                        if event['eventNs'] < 10**17 or abs(event['ts'] - event['eventNs']) > 86400*10**9:
                            raise ValueError('EVENT_TIME_UNIT_OR_RANGE_INVALID')
                        if count % 10000 == 0:
                            check(); quota_check()
                        first = event['ts'] if first is None else first
                        last = event['ts']; count += 1
                        writer.write((canonical(event)+'\n').encode())
                shards.append({'venue': venue, 'kind': kind, 'hour': hour.isoformat(), 'file': store_file(name),
                               'count': count, 'firstNs': first, 'lastNs': last, 'source': file['source']})
                progress({'phase': 'normalize', 'venue': venue, 'completed': file_index+1,
                          'total': len(venue_files), 'source': file['source']})
            except Exception as exc:
                raise ValueError(f'NORMALIZATION_FAILED: {file["source"]}: {exc}') from None
            finally:
                from pathlib import Path
                Path(name).unlink(missing_ok=True)
        quality[venue] = {'duplicateSnapshots': validator.duplicates, 'strictValidation': True}
    content = {'schemaVersion': 1, 'converterVersion': CONVERTER_VERSION, **config,
               'rules': rules, 'sources': [files[k] for k in sorted(files)], 'shards': shards,
               'quality': quality, 'timeUnits': {'received': 'ns', 'BINANCE.event': 'ms', 'OKX.event': 'ms'},
               'tieOrder': 'received time, BINANCE before OKX, book before trade, source message order'}
    content['id'] = digest(content)
    manifest_obj = write_json_object(content)
    directory = root() / 'datasets' / content['id']
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'manifest.json').write_text(canonical(content))
    (directory / 'COMPLETE').write_text(manifest_obj['sha256'])
    return content


def shard_events(shards):
    import io
    for shard in sorted(shards, key=lambda s: s['hour']):
        with resolve_object(shard['file']).open('rb') as src, zstandard.ZstdDecompressor().stream_reader(src) as reader:
            for line in io.TextIOWrapper(reader):
                yield json.loads(line)


def replay_events(manifest):
    streams = []
    for venue in VENUES:
        for kind in ('orderbook', 'trades'):
            streams.append(shard_events([s for s in manifest['shards'] if s['venue']==venue and s['kind']==kind]))
    # heapq.merge preserves stream index for equal keys, matching manifest tieOrder.
    yield from heapq.merge(*streams, key=lambda e: e['ts'])
