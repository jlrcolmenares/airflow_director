FROM apache/airflow:2.8.2rc2-python3.10 AS builder

USER airflow

RUN pip install chime

