FROM ubuntu:lunar

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    openjdk-20-jdk \
    python3 \
    python3-pip \
    python3-pandas

RUN pip3 install grpcio pyarrow pyspark --break-system-packages
RUN pip3 install google-api-python-client grpcio-status --break-system-packages