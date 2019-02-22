FROM rikorose/gcc-cmake:gcc-8
LABEL maintainer="mario.bielert@tu-dresden.de"

RUN useradd -m metricq
RUN apt-get update && apt-get install -y git libprotobuf-dev protobuf-compiler build-essential libssl-dev

RUN mkdir /var/hta
RUN chown metricq:metricq /var/hta

USER metricq

COPY --chown=metricq:metricq . /home/metricq/metricq-db-hta

RUN mkdir /home/metricq/metricq-db-hta/build

WORKDIR /home/metricq/metricq-db-hta/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 2

ARG token=metricq-db-hta
ENV token=$token

ARG metricq_url=amqp://localhost:5672
ENV metricq_url=$metricq_url

CMD /home/metricq/metricq-db-hta/build/metricq-db-hta --token $token --server $metricq_url -v
