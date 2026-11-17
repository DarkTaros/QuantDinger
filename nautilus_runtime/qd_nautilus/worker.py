"""Independent Celery queues with PostgreSQL claims and isolated subprocesses."""
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import quote
import psycopg2
from celery import Celery
from dotenv import load_dotenv
from qd_arbitrage import store

load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)
password = os.getenv('CELERY_REDIS_PASSWORD', os.getenv('REDIS_PASSWORD', ''))
auth = ':'+quote(password,safe='')+'@' if password else ''
broker = os.getenv('CELERY_BROKER_URL') or f"redis://{auth}{os.getenv('CELERY_REDIS_HOST','127.0.0.1')}:{os.getenv('CELERY_REDIS_PORT','6380')}/0"
app = Celery('qd_arbitrage', broker=broker)
app.conf.update(task_serializer='json', accept_content=['json'], task_acks_late=True,
                task_reject_on_worker_lost=True, worker_prefetch_multiplier=1,
                broker_transport_options={'visibility_timeout': 86400}, task_ignore_result=True,
                task_routes={'qd.arb.import': {'queue':'arbitrage_import'}, 'qd.arb.backtest': {'queue':'arbitrage_backtest'}})


def process_environment():
    env = os.environ.copy()
    # The child cannot discover live exchange credentials from its environment.
    for key in list(env):
        if key.startswith(('BINANCE_', 'OKX_')) and any(x in key for x in ('KEY','SECRET','PASSPHRASE')):
            env.pop(key)
    return env


def spawn(job_id):
    token = store.claim(job_id)
    if not token:
        return None
    log_dir = Path(os.getenv('ARB_DATA_DIR') or Path(__file__).resolve().parents[2] / 'data/arbitrage') / 'logs'
    log_dir.mkdir(parents=True,exist_ok=True)
    with (log_dir / (job_id+'.log')).open('a') as log:
        process = subprocess.Popen([sys.executable,'-m','qd_nautilus.runtime',job_id,token],
                                   env=process_environment(),stdout=log,stderr=log,start_new_session=True)
    return process, token


def monitor(job_id, process, token):
    stopped_at = None
    while process.poll() is None:
        try:
            store.heartbeat(job_id, token)
        except RuntimeError:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
            return
        row = store.job(job_id)
        if row['status'] == 'stopping':
            stopped_at = stopped_at or time.monotonic()
            if time.monotonic()-stopped_at > 45:
                process.kill()
            elif time.monotonic()-stopped_at > 30:
                process.terminate()
        time.sleep(2)
    row = store.job(job_id)
    if row['status'] in ('running','stopping'):
        store.finish(job_id,token,'interrupted',error='CHILD_EXITED_BEFORE_CONFIRMATION')


def finite(job_id, slot):
    # A session advisory lock enforces one backtest across all worker replicas.
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0))', (slot,))
            if not cur.fetchone()[0]:
                return
        result = spawn(job_id)
        if result:
            monitor(job_id, *result)
    finally:
        conn.close()


@app.task(name='qd.arb.backtest')
def run_backtest(job_id):
    finite(job_id, 'arb-backtest-slot')


@app.task(name='qd.arb.import')
def run_import(job_id):
    # Single importer already runs two bounded network downloads internally.
    finite(job_id, 'arb-import-slot')
