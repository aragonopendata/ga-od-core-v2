FROM postgres:17


COPY docker-entrypoint-initdb.d/create_gaodcore_read_user.sql /docker-entrypoint-initdb.d/

ENV TZ=Europe/Madrid
