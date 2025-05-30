FROM python:3.9-buster

ARG GAODCORE_DIR="/opt/gaodcore"
ARG ORACLE_DIR="/opt/oracle"

ARG ORACLE_INSTANT_CLIENT_URL="https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip"
ARG ORACLE_INSTANT_CLIENT_VERSION="21_1"
ARG ORACLE_INSTANT_CLIENT_TMP="/tmp/instantclient-basiclite-linux.zip"

RUN apt update
RUN apt upgrade -y
RUN apt install -y wget
RUN apt clean -y


# Install Oracle Instant client
RUN apt install libaio1 libaio-dev
RUN mkdir $ORACLE_DIR
RUN wget $ORACLE_INSTANT_CLIENT_URL -O $ORACLE_INSTANT_CLIENT_TMP
RUN unzip $ORACLE_INSTANT_CLIENT_TMP -d $ORACLE_DIR
RUN rm $ORACLE_INSTANT_CLIENT_TMP
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/oracle/instantclient_$ORACLE_INSTANT_CLIENT_VERSION"


#install MSSQL Instant 
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install FreeTDS and dependencies
RUN apt-get update -y
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN ACCEPT_EULA=Y apt-get install -y unixodbc-dev
RUN apt-get install -y unixodbc unixodbc-dev freetds-dev freetds-bin  freetds-dev tdsodbc
RUN apt-get install -y libgssapi-krb5-2
RUN apt install -y wget

RUN apt-get update
RUN apt install postgresql-client -y

RUN echo "[FreeTDS]\n\ 
    Description = FreeTDS unixODBC Driver\n\
    Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so" >> /etc/odbcinst.ini
RUN apt clean -y
RUN cp /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so /usr/local/lib/

RUN mkdir $GAODCORE_DIR
WORKDIR $GAODCORE_DIR


# Install MySQL
RUN apt-get install libmariadb-dev -y

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY ./src .
COPY ./scripts ./scripts
RUN chmod +x scripts/create_requests_view.sh 

CMD bash -c "python manage.py migrate --noinput \
    && python manage.py collectstatic --noinput \
    && python manage.py createcachetable \
    && bash ./scripts/create_requests_view.sh \
    && gunicorn gaodcore_project.wsgi --bind :8000 --workers 9 --timeout 240"