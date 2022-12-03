FROM docker.io/bitnami/spark:3.3

USER root

EXPOSE 8888

RUN pip install --no-cache-dir jupyterlab pyspark confluent_kafka \
    && useradd -ms /bin/bash lab

WORKDIR /home/lab

ENTRYPOINT jupyter lab --ip=0.0.0.0 --allow-root
