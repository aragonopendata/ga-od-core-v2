FROM python:3.10-bookworm

ARG GAODCORE_DIR="/opt/gaodcore"
ARG ORACLE_DIR="/opt/oracle"

ARG ORACLE_INSTANT_CLIENT_URL="https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip"
ARG ORACLE_INSTANT_CLIENT_VERSION="21_1"
ARG ORACLE_INSTANT_CLIENT_TMP="/tmp/instantclient-basiclite-linux.zip"

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y wget libaio1 libaio-dev unzip \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*


# Install Oracle Instant client (needed for python-oracledb Thick mode)
# Required for older Oracle databases with unsupported password verifiers
RUN mkdir -p $ORACLE_DIR
RUN wget $ORACLE_INSTANT_CLIENT_URL -O $ORACLE_INSTANT_CLIENT_TMP
RUN unzip $ORACLE_INSTANT_CLIENT_TMP -d $ORACLE_DIR
RUN rm $ORACLE_INSTANT_CLIENT_TMP
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/oracle/instantclient_$ORACLE_INSTANT_CLIENT_VERSION"


# Add the Microsoft repository (debian/12 matches the bookworm base image)
RUN mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list

# Install MSSQL tools, FreeTDS, PostgreSQL and MySQL client libraries
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y \
        mssql-tools \
        msodbcsql17 \
        unixodbc \
        unixodbc-dev \
        freetds-dev \
        freetds-bin \
        tdsodbc \
        libgssapi-krb5-2 \
        postgresql-client \
        libmariadb-dev \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

RUN echo "[FreeTDS]\n\
    Description = FreeTDS unixODBC Driver\n\
    Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
    Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so" >> /etc/odbcinst.ini

# Configure FreeTDS with proper settings
RUN echo "[global]\n\
    tds version = 8.0\n\
    client charset = UTF-8\n\
    text size = 64512\n\
    connect timeout = 10" > /etc/freetds/freetds.conf

RUN cp /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so /usr/local/lib/

RUN mkdir $GAODCORE_DIR
WORKDIR $GAODCORE_DIR

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY ./src .
COPY ./scripts ./scripts
RUN chmod +x scripts/create_requests_view.sh

CMD bash -c "python manage.py migrate --noinput \
    && python manage.py collectstatic --noinput \
    && python manage.py createcachetable \
    && gunicorn gaodcore_project.wsgi --bind :8000 --workers 9 --timeout 240 --max-requests 1000 --max-requests-jitter 100"
