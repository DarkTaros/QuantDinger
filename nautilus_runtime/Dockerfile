FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt/qd:/opt/qd/runtime ARB_DATA_DIR=/data/arbitrage
WORKDIR /opt/qd
COPY nautilus_runtime/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock && useradd -u 10001 -m arbitrage && mkdir -p /data/arbitrage && chown arbitrage /data/arbitrage
COPY backend_api_python/qd_arbitrage /opt/qd/qd_arbitrage
COPY nautilus_runtime/qd_nautilus /opt/qd/runtime/qd_nautilus
USER arbitrage
CMD ["python", "-m", "qd_nautilus.supervisor"]
