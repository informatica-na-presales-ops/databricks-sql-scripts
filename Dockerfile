FROM ghcr.io/astral-sh/uv:0.9.5-trixie-slim

RUN /usr/sbin/useradd --create-home --shell /bin/bash --user-group python

ARG DEBIAN_FRONTEND=noninteractive
RUN /usr/bin/apt-get update \
 && /usr/bin/apt-get install --assume-yes \
    	# required for psycopg2
        postgresql-common \
 && rm -rf /var/lib/apt/lists/*

USER python

WORKDIR /app
COPY --chown=python:python .python-version pyproject.toml uv.lock ./
RUN /usr/local/bin/uv sync --frozen

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

COPY --chown=python:python dbx ./dbx
COPY --chown=python:python pg ./pg
COPY --chown=python:python get-iics-agents.py ./
COPY --chown=python:python get-iics-organizations.py ./
COPY --chown=python:python get-iics-user-weekly-logins.py ./
COPY --chown=python:python get-iics-user-roles.py ./
COPY --chown=python:python get-iics-users.py ./

LABEL org.opencontainers.image.source="https://github.com/informatica-na-presales-ops/databricks-sql-scripts"
