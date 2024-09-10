FROM python:3.12.6-slim

RUN /usr/sbin/useradd --create-home --shell /bin/bash --user-group python

ARG DEBIAN_FRONTEND=noninteractive
RUN /usr/bin/apt-get update \
 && /usr/bin/apt-get install --assume-yes \
    	# required for psycopg2
        postgresql-common \
 && rm -rf /var/lib/apt/lists/*

USER python
RUN /usr/local/bin/python -m venv /home/python/venv

COPY --chown=python:python requirements.txt /home/python/databricks-sql-scripts/requirements.txt
RUN /home/python/venv/bin/pip install --no-cache-dir --requirement /home/python/databricks-sql-scripts/requirements.txt

ENV PATH="/home/python/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

WORKDIR /home/python/databricks-sql-scripts

COPY --chown=python:python dbx /home/python/databricks-sql-scripts/dbx
COPY --chown=python:python pg /home/python/databricks-sql-scripts/pg
COPY --chown=python:python get-iics-agents.py /home/python/databricks-sql-scripts/get-iics-agents.py
COPY --chown=python:python get-iics-organizations.py /home/python/databricks-sql-scripts/get-iics-organizations.py
COPY --chown=python:python get-iics-user-weekly-logins.py /home/python/databricks-sql-scripts/get-iics-user-weekly-logins.py
COPY --chown=python:python get-iics-user-roles.py /home/python/databricks-sql-scripts/get-iics-user-roles.py
COPY --chown=python:python get-iics-users.py /home/python/databricks-sql-scripts/get-iics-users.py

LABEL org.opencontainers.image.source="https://github.com/informatica-na-presales-ops/databricks-sql-scripts"
