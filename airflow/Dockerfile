FROM apache/airflow:3.0.6

USER root

RUN apt-get update && apt-get install -y chromium chromium-driver
RUN apt-get install -y wget unzip curl fonts-liberation libnss3 libxss1 libasound2

USER airflow

RUN pip install --no-cache-dir blinker==1.7.0
RUN pip install --no-cache-dir selenium-wire