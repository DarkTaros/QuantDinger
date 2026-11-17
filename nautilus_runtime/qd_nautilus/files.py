"""Content-addressed storage and bounded, credential-safe provider downloads."""
import fcntl
import hashlib
import os
from pathlib import Path
import tempfile
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.request import getproxies
import httpx
import zstandard
from qd_arbitrage.contracts import canonical

CONVERTER_VERSION = '1'


def root():
    path = Path(os.getenv('ARB_DATA_DIR') or Path(__file__).resolve().parents[2] / 'data/arbitrage')
    path.mkdir(parents=True, exist_ok=True)
    return path


def client():
    # Explicit proxy avoids macOS system NO_PROXY IPv6 parsing failures in httpx.
    return httpx.Client(trust_env=False, proxy=os.getenv('ARB_HTTP_PROXY') or getproxies().get('https'),
                        timeout=httpx.Timeout(90, connect=30), follow_redirects=True)


def sha_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def quota_check(extra=0):
    used = sum(p.stat().st_size for p in root().rglob('*') if p.is_file())
    if used + extra > int(os.getenv('ARB_QUOTA_BYTES', str(100 * 1024**3))):
        raise RuntimeError('DATA_QUOTA_EXCEEDED')


def store_file(path):
    digest = sha_file(path)
    dest = root() / 'objects' / digest[:2] / digest
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Atomic no-overwrite installation. Existing hashes remain immutable.
    try:
        os.link(path, dest)
    except FileExistsError:
        if sha_file(dest) != digest:
            raise RuntimeError('STORED_OBJECT_CORRUPTED')
    Path(path).unlink()
    return {'sha256': digest, 'path': str(dest.relative_to(root())), 'bytes': dest.stat().st_size}


def resolve_object(obj):
    path = root() / 'objects' / obj['sha256'][:2] / obj['sha256']
    if not path.is_file() or sha_file(path) != obj['sha256']:
        raise RuntimeError('OBJECT_HASH_MISMATCH')
    return path


def retry_after(value, attempt):
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError):
            seconds = 2 ** attempt
    if seconds > 300:
        raise RuntimeError('PROVIDER_RATE_LIMIT_RETRY_LATER')
    return max(0, seconds)


def download(object_path, check=lambda: None):
    params = {'file': object_path}
    if os.getenv('CRYPTOHFTDATA_API_KEY'):
        params['api_key'] = os.environ['CRYPTOHFTDATA_API_KEY']
    scratch = root() / 'scratch'
    scratch.mkdir(exist_ok=True)
    # A file lock bounds quota races across worker processes; two network downloads
    # are permitted, with per-chunk quota checks under this shared lock.
    last_error = 'NETWORK_ERROR'
    for attempt in range(4):
        check()
        fd, name = tempfile.mkstemp(dir=scratch)
        os.close(fd)
        try:
            with client() as http, http.stream('GET', 'https://api.cryptohftdata.com/v1/download', params=params) as response:
                status = response.status_code
                if status in (401, 403):
                    raise RuntimeError(f'PROVIDER_AUTH_FAILED: {object_path}')
                if status == 404:
                    raise FileNotFoundError(f'MISSING_HOUR: {object_path}')
                if status == 429 or status >= 500:
                    last_error = f'PROVIDER_HTTP_{status}'
                    delay = retry_after(response.headers.get('Retry-After'), attempt)
                    for _ in range(int(delay) + 1):
                        check(); time.sleep(min(1, delay)); delay = max(0, delay - 1)
                    continue
                if status != 200:
                    raise RuntimeError(f'PROVIDER_HTTP_{status}: {object_path}')
                with open(name, 'wb') as out, open(root() / '.quota.lock', 'a') as lock:
                    for chunk in response.iter_bytes(1024 * 1024):
                        check()
                        fcntl.flock(lock, fcntl.LOCK_EX)
                        try:
                            quota_check(len(chunk)); out.write(chunk); out.flush()
                        finally:
                            fcntl.flock(lock, fcntl.LOCK_UN)
            # `store_file` unlinks the scratch path after atomically linking the
            # immutable object. Keep each temporary name local to this attempt;
            # a retry or a concurrent download must never share it.
            raw = store_file(name)
            name = None
            path = resolve_object(raw)
            with path.open('rb') as src:
                magic = src.read(4)
            if magic == b'PAR1':
                parquet = raw
            elif magic == b'\x28\xb5\x2f\xfd':
                fd, name = tempfile.mkstemp(dir=scratch); os.close(fd)
                with path.open('rb') as src, zstandard.ZstdDecompressor().stream_reader(src) as reader, open(name, 'wb') as out:
                    while chunk := reader.read(1024 * 1024):
                        check(); quota_check(len(chunk)); out.write(chunk); out.flush()
                with open(name, 'rb') as test:
                    if test.read(4) != b'PAR1':
                        raise RuntimeError('INVALID_PARQUET_HEADER')
                parquet = store_file(name)
                name = None
            else:
                raise RuntimeError(f'UNEXPECTED_FILE_FORMAT: {object_path}')
            return {'source': object_path, 'downloadedAt': datetime.now(timezone.utc).isoformat(),
                    'raw': raw, 'parquet': parquet, 'converterVersion': CONVERTER_VERSION}
        except httpx.HTTPError:
            # Never persist exception strings: they may include authenticated URLs.
            last_error = 'PROVIDER_NETWORK_ERROR'
            time.sleep(2 ** attempt)
        finally:
            if name:
                Path(name).unlink(missing_ok=True)
    raise RuntimeError(f'{last_error}: {object_path}')


def write_json_object(value):
    fd, name = tempfile.mkstemp(dir=root())
    with os.fdopen(fd, 'w') as out:
        out.write(canonical(value)); out.flush(); os.fsync(out.fileno())
    return store_file(name)
