FROM ubuntu:lunar

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    openjdk-20-jdk \
    python3 \
    python3-pip

RUN pip3 install pyspark --break-system-packages