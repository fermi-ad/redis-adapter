FROM ubuntu:latest

RUN apt-get update -y
RUN apt-get install -y wget cmake make g++ vim nano

WORKDIR /usr/src
RUN wget https://download.redis.io/redis-stable.tar.gz
RUN tar -xvf redis-stable.tar.gz redis-stable
RUN rm redis-stable.tar.gz

WORKDIR /usr/src/redis-stable
RUN make install

COPY . /usr/src/RedisAdapter

WORKDIR /usr/src/RedisAdapter/build
RUN rm -rf *
RUN cmake .. -DREDIS_ADAPTER_TEST=1
RUN make install

#CMD tail -f /dev/null

LABEL Name=RedisAdapter Version=0.0.1
