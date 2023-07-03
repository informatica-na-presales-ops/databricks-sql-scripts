FROM python:3.11.4-slim-bookworm

RUN /usr/sbin/useradd --create-home --shell /bin/bash --user-group python

ARG DEBIAN_FRONTEND=noninteractive
RUN /usr/bin/apt-get update \
 && /usr/bin/apt-get install --assume-yes \
    	# required for psycopg2
        postgresql-common \
 && rm -rf /var/lib/apt/lists/*

USER python
RUN /usr/local/bin/python -m venv /home/python/venv

COPY --chown=python:python requirements.txt /home/pthon/databricks-sql-scripts/requirements.txt
RUN /home/python/venv/bin/pip install --no-cache-dir --requirement /home/pthon/databricks-sql-scripts/requirements.txt

ENV PATH="/home/python/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

WORKDIR /home/python/databricks-sql-scripts

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.source="https://bitb.informatica.com/projects/TS/repos/databricks-sql-scripts"
