FROM flink:scala_2.12-java21

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER flink