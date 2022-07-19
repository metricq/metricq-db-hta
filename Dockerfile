FROM debian:bullseye AS builder
LABEL maintainer="mario.bielert@tu-dresden.de"

RUN useradd -m metricq
RUN apt-get update && apt-get install -y \
      git \
      libprotobuf-dev \
      protobuf-compiler \
      build-essential \
      cmake \
      libssl-dev \
      && rm -rf /var/lib/apt/lists/*

USER metricq

COPY --chown=metricq:metricq . /home/metricq/metricq-db-hta

RUN mkdir /home/metricq/metricq-db-hta/build

WORKDIR /home/metricq/metricq-db-hta/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 2
RUN make package

FROM debian:bullseye
LABEL maintainer="mario.bielert@tu-dresden.de"

RUN useradd -m metricq
RUN apt-get update && apt-get install -y \
      libssl1.1 \
      libprotobuf23 \
      tzdata \
      wget \
      && rm -rf /var/lib/apt/lists/*

COPY --chown=metricq:metricq --from=builder /home/metricq/metricq-db-hta/build/metricq-db-hta-*-Linux.sh /home/metricq/metricq-db-hta-Linux.sh

USER root
RUN /home/metricq/metricq-db-hta-Linux.sh --skip-license --prefix=/usr
RUN mkdir /var/hta
RUN chown metricq:metricq /var/hta

USER metricq
WORKDIR /home/metricq
RUN wget -O wait-for-it.sh https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh && chmod +x wait-for-it.sh

ARG token=metricq-db-hta
ENV token=$token

ARG metricq_url=amqp://localhost:5672
ENV metricq_url=$metricq_url

ARG wait_for_rabbitmq_url=127.0.0.1:5672
ENV wait_for_rabbitmq_url=$wait_for_rabbitmq_url

CMD  /home/metricq/wait-for-it.sh $wait_for_rabbitmq_url -- /usr/bin/metricq-db-hta --token $token --server $metricq_url -v
