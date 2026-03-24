FROM python:3.13.12

USER root

WORKDIR /opt/news-aggregator
COPY . /opt/news-aggregator

RUN apt-get update -y
RUN apt-get install -y tini

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="${PATH}:/root/.local/bin"
RUN uv sync --no-dev
ENV PATH="${PATH}:/opt/news-aggregator/.venv/bin"

ENV PYTHONPATH="/opt/news-aggregator"
ENV DAGSTER_HOME=/opt/news-aggregator/dagster_src
