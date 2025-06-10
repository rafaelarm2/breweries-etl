FROM apache/airflow:3.0.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock* /opt/airflow/

WORKDIR /opt/airflow

USER airflow

RUN pip install "apache-airflow==3.0.1" && \
if [ -f poetry.lock ]; then \
    pip install poetry-plugin-export && \
    poetry export --without-hashes --format=requirements.txt > requirements.txt && \
    pip install -r requirements.txt && \
    rm -f requirements.txt; \
fi
