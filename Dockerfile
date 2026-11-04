FROM python:3.8.20-slim-bookworm@sha256:1d52838af602b4b5a831beb13a0e4d073280665ea7be7f69ce2382f29c5a613f AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --create-home app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.txt && python -m pip check

FROM base AS test
COPY requirements-dev.txt ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-dev.txt && python -m pip check
COPY --chown=app:app . .
RUN chown app:app /app
USER app
CMD ["sh", "-c", "ruff check . && ruff format --check . && pytest -q --cov=app --cov-report=term-missing"]

FROM base AS runtime
COPY --chown=app:app app ./app
USER app
ENV PORT=8000
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8000')+'/health', timeout=3)"
CMD ["python", "-m", "app"]
