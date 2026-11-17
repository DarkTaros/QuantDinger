"""Transactional outbox, tenant scopes and fenced state transitions."""
import os
import uuid
from contextlib import contextmanager
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from .contracts import canonical

TERMINAL = ('succeeded', 'failed', 'cancelled', 'interrupted')


@contextmanager
def connection():
    conn = psycopg2.connect(os.environ['DATABASE_URL'], cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
            yield conn
    finally:
        conn.close()


def submit(user_id, kind, config, idempotency_key=None):
    if idempotency_key and (not isinstance(idempotency_key, str) or len(idempotency_key) > 120):
        raise ValueError('INVALID_IDEMPOTENCY_KEY')
    job_id, run_id = uuid.uuid4().hex, uuid.uuid4().hex if kind != 'arb_import' else None
    with connection() as conn, conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                    (f'arb-submit:{user_id}:{kind}:{idempotency_key}',))
        if idempotency_key:
            cur.execute('SELECT job_id, request FROM qd_agent_jobs WHERE user_id=%s AND kind=%s AND idempotency_key=%s',
                        (user_id, kind, idempotency_key))
            old = cur.fetchone()
            if old:
                if canonical(old['request']) != canonical(config):
                    raise ValueError('IDEMPOTENCY_CONFLICT')
                cur.execute('SELECT run_id FROM qd_arb_controls WHERE job_id=%s', (old['job_id'],))
                return {'jobId': old['job_id'], 'runId': cur.fetchone()['run_id'], 'duplicate': True}
        if kind == 'arb_backtest':
            cur.execute('SELECT d.* FROM qd_arb_datasets d JOIN qd_arb_dataset_access a ON a.dataset_id=d.id WHERE d.id=%s AND a.user_id=%s',
                        (config['datasetId'], user_id))
            dataset = cur.fetchone()
            if not dataset:
                raise ValueError('DATASET_NOT_FOUND')
            if dataset['pair'] != config['pair'] or dataset['start_at'] > datetime.fromisoformat(config['start']) or dataset['end_at'] < datetime.fromisoformat(config['end']):
                raise ValueError('DATASET_COVERAGE_MISMATCH')
        if kind == 'arb_paper':
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('arb-paper-capacity',0))")
            cur.execute("SELECT 1 FROM qd_arb_runs WHERE mode='paper' AND status IN ('queued','running','stopping')")
            if cur.fetchone():
                raise ValueError('PAPER_CAPACITY_REACHED')
        cur.execute("INSERT INTO qd_agent_jobs(job_id,user_id,kind,status,request,idempotency_key) VALUES (%s,%s,%s,'queued',%s,%s)",
                    (job_id, user_id, kind, Json(config), idempotency_key))
        if run_id:
            cur.execute('INSERT INTO qd_arb_runs(id,user_id,job_id,mode,config,dataset_id) VALUES (%s,%s,%s,%s,%s,%s)',
                        (run_id, user_id, job_id, config['mode'], Json(config), config.get('datasetId')))
        cur.execute('INSERT INTO qd_arb_controls(job_id,run_id) VALUES (%s,%s)', (job_id, run_id))
    return {'jobId': job_id, 'runId': run_id, 'status': 'queued'}


def job(job_id, user_id=None):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('SELECT j.*, c.run_id FROM qd_agent_jobs j JOIN qd_arb_controls c USING(job_id) WHERE j.job_id=%s' +
                    (' AND j.user_id=%s' if user_id is not None else ''),
                    (job_id, user_id) if user_id is not None else (job_id,))
        return cur.fetchone()


def claim(job_id):
    token = uuid.uuid4().hex
    with connection() as conn, conn.cursor() as cur:
        cur.execute('SELECT j.status FROM qd_agent_jobs j JOIN qd_arb_controls c USING(job_id) WHERE j.job_id=%s FOR UPDATE OF j,c', (job_id,))
        row = cur.fetchone()
        if not row or row['status'] != 'queued':
            return None
        cur.execute("UPDATE qd_agent_jobs SET status='running',started_at=now() WHERE job_id=%s", (job_id,))
        cur.execute("UPDATE qd_arb_controls SET token=%s,lease_until=now()+interval '90 seconds',attempts=attempts+1 WHERE job_id=%s", (token, job_id))
        cur.execute("UPDATE qd_arb_runs SET status='running' WHERE job_id=%s", (job_id,))
    return token


def guard(cur, job_id, token, allow_stopping=False):
    cur.execute('SELECT j.status,c.lease_until>now() AS valid FROM qd_agent_jobs j JOIN qd_arb_controls c USING(job_id) WHERE j.job_id=%s AND c.token=%s FOR UPDATE OF j,c', (job_id, token))
    row = cur.fetchone()
    if not row or not row['valid'] or row['status'] not in (('running', 'stopping') if allow_stopping else ('running',)):
        raise RuntimeError('LEASE_LOST_OR_CANCELLED')


def heartbeat(job_id, token):
    with connection() as conn, conn.cursor() as cur:
        guard(cur, job_id, token, True)
        cur.execute("UPDATE qd_arb_controls SET lease_until=now()+interval '90 seconds' WHERE job_id=%s", (job_id,))


def progress(job_id, token, value):
    with connection() as conn, conn.cursor() as cur:
        guard(cur, job_id, token, True)
        cur.execute('UPDATE qd_agent_jobs SET progress=%s WHERE job_id=%s', (Json(value), job_id))


def append_records(job_id, token, run_id, events=(), cycles=(), summary=None, series=()):
    with connection() as conn, conn.cursor() as cur:
        guard(cur, job_id, token, True)
        cur.execute('SELECT 1 FROM qd_arb_runs WHERE id=%s AND job_id=%s', (run_id, job_id))
        if not cur.fetchone():
            raise RuntimeError('RUN_JOB_MISMATCH')
        for table, rows in [('qd_arb_events', events), ('qd_arb_cycles', cycles)]:
            for seq, body in rows:
                query = f'INSERT INTO {table}(run_id,seq,body) VALUES (%s,%s,%s) ON CONFLICT(run_id,seq) DO UPDATE SET body=EXCLUDED.body'
                if table == 'qd_arb_events':
                    query += f' WHERE {table}.body=EXCLUDED.body'
                cur.execute(query, (run_id, seq, Json(body)))
                if cur.rowcount != 1:
                    raise RuntimeError('EVENT_SEQUENCE_CONFLICT')
        for point in series:
            cur.execute("""INSERT INTO qd_arb_series(run_id,metric,direction,seq,ts_ns,value,unit)
                VALUES(%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(run_id,metric,direction,seq) DO UPDATE SET
                  ts_ns=EXCLUDED.ts_ns,value=EXCLUDED.value,unit=EXCLUDED.unit""",
                        (run_id, point['metric'], point.get('direction', ''), point['seq'],
                         point['tsNs'], point.get('value'), point.get('unit', '')))
        if summary is not None:
            cur.execute('UPDATE qd_arb_runs SET summary=%s WHERE id=%s AND job_id=%s', (Json(summary), run_id, job_id))


def finish(job_id, token, status, result=None, error=None):
    if status not in TERMINAL:
        raise ValueError('INVALID_TERMINAL_STATUS')
    with connection() as conn, conn.cursor() as cur:
        guard(cur, job_id, token, True)
        cur.execute('SELECT status FROM qd_agent_jobs WHERE job_id=%s', (job_id,))
        if cur.fetchone()['status'] == 'stopping' and status == 'succeeded':
            status = 'cancelled'
        cur.execute('UPDATE qd_agent_jobs SET status=%s,result=%s,error=%s,finished_at=now() WHERE job_id=%s', (status, Json(result), error, job_id))
        cur.execute('UPDATE qd_arb_runs SET status=%s,error=%s,finished_at=now() WHERE job_id=%s', (status, error, job_id))
        cur.execute('UPDATE qd_arb_controls SET lease_until=NULL WHERE job_id=%s', (job_id,))


def cancel(job_id, user_id):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('SELECT j.status FROM qd_agent_jobs j JOIN qd_arb_controls c USING(job_id) WHERE j.job_id=%s AND j.user_id=%s FOR UPDATE OF j,c', (job_id, user_id))
        row = cur.fetchone()
        if not row:
            return None
        status = row['status']
        if status not in TERMINAL:
            status = 'cancelled' if status == 'queued' else 'stopping'
            cur.execute("UPDATE qd_agent_jobs SET status=%s,finished_at=CASE WHEN %s='cancelled' THEN now() ELSE finished_at END WHERE job_id=%s", (status, status, job_id))
            cur.execute("UPDATE qd_arb_runs SET status=%s,stop_requested=true,finished_at=CASE WHEN %s='cancelled' THEN now() ELSE finished_at END WHERE job_id=%s", (status, status, job_id))
        return {'status': status}


def runs(user_id, run_id=None):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('SELECT * FROM qd_arb_runs WHERE user_id=%s' + (' AND id=%s' if run_id else ' ORDER BY created_at DESC LIMIT 100'), (user_id, run_id) if run_id else (user_id,))
        return cur.fetchone() if run_id else cur.fetchall()


def records(user_id, run_id, table, after=0, limit=100):
    if table not in ('cycles', 'events'):
        raise ValueError('INVALID_RECORD_TYPE')
    with connection() as conn, conn.cursor() as cur:
        cur.execute(f'SELECT e.seq,e.body FROM qd_arb_{table} e JOIN qd_arb_runs r ON e.run_id=r.id WHERE r.user_id=%s AND r.id=%s AND e.seq>%s ORDER BY e.seq LIMIT %s', (user_id, run_id, after, limit + 1))
        rows = cur.fetchall()
        return {'items': rows[:limit], 'nextCursor': rows[limit-1]['seq'] if len(rows) > limit else None}


def series(user_id, run_id, metric, direction='', after=0, limit=600):
    allowed = {'equity', 'drawdown', 'spread', 'fees', 'exposure'}
    if metric not in allowed or not 0 <= after or not 1 <= limit <= 2000:
        raise ValueError('INVALID_SERIES_QUERY')
    with connection() as conn, conn.cursor() as cur:
        # Downsample in PostgreSQL so a long L2 run never transfers every
        # checkpoint to the browser.  The first and last point are retained.
        cur.execute("""WITH ordered AS (
              SELECT s.seq,s.ts_ns,s.value,s.unit,s.direction,
                     row_number() OVER (ORDER BY s.seq) AS rn,
                     count(*) OVER () AS total
                FROM qd_arb_series s JOIN qd_arb_runs r ON r.id=s.run_id
               WHERE r.user_id=%s AND s.run_id=%s AND s.metric=%s AND s.direction=%s
                 AND s.seq>%s
            ), sampled AS (
              SELECT * FROM ordered
               WHERE rn=1 OR rn=total OR mod(rn-1, GREATEST(1, CEIL(total::numeric/%s)::bigint))=0
               ORDER BY seq
               LIMIT %s
            ) SELECT * FROM sampled ORDER BY seq""",
                    (user_id, run_id, metric, direction, after, limit, limit + 1))
        rows = cur.fetchall()
        items = [{'seq': row['seq'], 'tsNs': row['ts_ns'], 'value': row['value'],
                  'unit': row['unit'], 'direction': row['direction']} for row in rows[:limit]]
        return {'items': items, 'nextCursor': rows[limit-1]['seq'] if len(rows) > limit else None}


def datasets(user_id):
    with connection() as conn, conn.cursor() as cur:
        cur.execute('SELECT d.id,d.pair,d.start_at,d.end_at,d.manifest,d.created_at FROM qd_arb_datasets d JOIN qd_arb_dataset_access a ON d.id=a.dataset_id WHERE a.user_id=%s ORDER BY d.created_at DESC LIMIT 100', (user_id,))
        return cur.fetchall()


def save_dataset(job_id, token, manifest):
    row = job(job_id)
    with connection() as conn, conn.cursor() as cur:
        guard(cur, job_id, token)
        cur.execute('INSERT INTO qd_arb_datasets(id,user_id,pair,start_at,end_at,manifest) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', (manifest['id'], row['user_id'], manifest['pair'], manifest['start'], manifest['end'], Json(manifest)))
        cur.execute('INSERT INTO qd_arb_dataset_access(dataset_id,user_id) VALUES(%s,%s) ON CONFLICT DO NOTHING', (manifest['id'], row['user_id']))


def pending_dispatch():
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT c.job_id,j.kind FROM qd_arb_controls c JOIN qd_agent_jobs j USING(job_id) WHERE j.status='queued' AND (c.dispatched_at IS NULL OR c.dispatched_at < now()-interval '30 seconds') ORDER BY j.created_at LIMIT 20 FOR UPDATE OF c SKIP LOCKED")
        rows = cur.fetchall()
        for row in rows:
            cur.execute('UPDATE qd_arb_controls SET dispatched_at=now() WHERE job_id=%s', (row['job_id'],))
        return rows


def reap_expired():
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT j.job_id FROM qd_agent_jobs j JOIN qd_arb_controls c USING(job_id) WHERE j.status IN ('running','stopping') AND c.lease_until<now() FOR UPDATE OF j,c SKIP LOCKED")
        for row in cur.fetchall():
            cur.execute("UPDATE qd_agent_jobs SET status='interrupted',error='WORKER_LEASE_EXPIRED',finished_at=now() WHERE job_id=%s", (row['job_id'],))
            cur.execute("UPDATE qd_arb_runs SET status='interrupted',error='WORKER_LEASE_EXPIRED',finished_at=now() WHERE job_id=%s", (row['job_id'],))
