FROM python:3.8

ARG ORACLE_DIR="/opt/oracle"

ARG ORACLE_INSTANT_CLIENT_URL="https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip"
ARG ORACLE_INSTANT_CLIENT_VERSION="21_1"
ARG ORACLE_INSTANT_CLIENT_TMP="/tmp/instantclient-basiclite-linux.zip"

WORKDIR /usr/src/app

RUN apt update
RUN apt upgrade -y
RUN apt install -y wget
RUN apt clean -y

# Install Oracle Instant client
RUN mkdir $ORACLE_DIR
RUN wget $ORACLE_INSTANT_CLIENT_URL -O $ORACLE_INSTANT_CLIENT_TMP
RUN unzip $ORACLE_INSTANT_CLIENT_TMP -d $ORACLE_DIR
RUN rm $ORACLE_INSTANT_CLIENT_TMP
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/oracle/instantclient_$ORACLE_INSTANT_CLIENT_VERSION"


COPY requirements.txt ./
COPY ./src .

RUN pip install -r requirements.txt
