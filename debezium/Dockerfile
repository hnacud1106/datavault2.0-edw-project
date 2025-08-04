FROM debezium/connect:2.4

ENV KAFKA_CONNECT_PLUGINS_DIR=/kafka/connect

USER root

RUN microdnf install -y curl && microdnf clean all

RUN curl -L -o /kafka/connect/clickhouse-jdbc.jar \
    "https://api.github.com/repos/ClickHouse/clickhouse-java/releases/download/v0.4.6/clickhouse-jdbc-0.4.6-all.jar"

USER kafka
