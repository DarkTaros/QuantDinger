"""Durable outbox dispatcher and long-lived paper-session supervisor."""
import os
import signal
import time
import psycopg2
from qd_arbitrage import store
from .worker import app, spawn


def main():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtextextended('arb-supervisor',0))")
        if not cur.fetchone()[0]:
            raise SystemExit('ARBITRAGE_SUPERVISOR_ALREADY_RUNNING')
    running, stopping = {}, False
    def stop(signum, frame):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not stopping or running:
            store.reap_expired()
            if not stopping:
                for row in store.pending_dispatch():
                    job_id, kind = row['job_id'], row['kind']
                    if kind == 'arb_paper':
                        if not running:
                            result = spawn(job_id)
                            if result:
                                running[job_id] = (*result, None)
                    else:
                        try:
                            app.send_task('qd.arb.import' if kind=='arb_import' else 'qd.arb.backtest',args=[job_id],queue='arbitrage_import' if kind=='arb_import' else 'arbitrage_backtest')
                        except Exception:
                            # The durable outbox will retry after its dispatch lease.
                            pass
            for job_id,(process,token,stop_time) in list(running.items()):
                row = store.job(job_id)
                if stopping and row['status']=='running':
                    store.cancel(job_id,row['user_id'])
                if process.poll() is not None:
                    if row['status'] in ('running','stopping'):
                        store.finish(job_id,token,'interrupted',error='PAPER_PROCESS_EXITED')
                    del running[job_id]; continue
                try:
                    store.heartbeat(job_id,token)
                except RuntimeError:
                    process.terminate(); continue
                if row['status']=='stopping' or stopping:
                    stop_time = stop_time or time.monotonic()
                    running[job_id] = (process,token,stop_time)
                    if time.monotonic()-stop_time>45:
                        process.kill()
                    elif time.monotonic()-stop_time>30:
                        process.terminate()
            time.sleep(2)
    finally:
        for process,token,_ in running.values():
            process.terminate()
        conn.close()


if __name__ == '__main__':
    main()
