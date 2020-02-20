FROM python:3

RUN mkdir -p /app

COPY healthcheck /app/healthcheck
COPY hc /app/hc

# modify content or change name of the configuration file
COPY config.ini /app/config.ini

WORKDIR /app

ENTRYPOINT [ "./hc" ]
