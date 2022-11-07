# GCC support can be specified at major, minor, or micro version
# (e.g. 8, 8.2 or 8.2.0).
# See https://hub.docker.com/r/library/gcc/ for all supported GCC
# tags from Docker Hub.
# See https://docs.docker.com/samples/library/gcc/ for more on how to use this image
FROM ubuntu:latest

# These commands copy your files into the specified directory in the image
# and set that as the working location

RUN apt update -y
RUN apt install -y make git wget cmake gcc g++ libconfig++-dev vim


WORKDIR /usr/src/redisAdapter
RUN wget https://download.redis.io/redis-stable.tar.gz
RUN tar -xvf redis-stable.tar.gz redis-stable

RUN git clone https://github.com/redis/hiredis.git
RUN git clone https://github.com/sewenew/redis-plus-plus.git
RUN git clone http://cdcvs.fnal.gov/projects/trace-git trace


#BUILD Redis Stable
WORKDIR /usr/src/redisAdapter/redis-stable
RUN make
RUN make install
#BUILD Hiredis
WORKDIR /usr/src/redisAdapter/hiredis
RUN make
RUN make install
#BUILD Redis plus plus
WORKDIR /usr/src/redisAdapter/redis-plus-plus
RUN mkdir build
WORKDIR /usr/src/redisAdapter/redis-plus-plus/build
RUN cmake ..
RUN make
RUN make install
#Build Trace
WORKDIR /usr/src/redisAdapter/trace
RUN make OUT=$PWD -j4 
RUN cp -r include/TRACE /usr/include/


RUN mkdir /usr/local/lib64

COPY . /usr/src/redisAdapter
WORKDIR /usr/src/redisAdapter
RUN make 
RUN make install
RUN make test


ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64

# This command compiles your app using GCC, adjust for your source code

#RUN make redisAdapterEngine

# This command runs your application, comment out this line to compile only
CMD [tail -f /dev/null]
#CMD ["./redisAdapterEngine -c bpmd.conf"]

LABEL Name=redisAdapter Version=0.0.1
