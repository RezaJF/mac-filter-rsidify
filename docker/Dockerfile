FROM python:3.11-slim

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# System utilities required by the MAC-filtering / rsIDify pipeline:
#   gawk  — MAC/MAF filtering and LDSC column extraction
#   bc    — MAF threshold parsing
#   gzip  — compressing output files
RUN apt-get update && \
    apt-get install -y --no-install-recommends gawk bc gzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY scripts /scripts
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PATH="/scripts:${PATH}"
ENV PYTHONPATH="/scripts"
