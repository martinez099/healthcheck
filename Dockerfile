FROM python:3

RUN mkdir -p /app

WORKDIR /app

COPY healthcheck healthcheck
COPY hc hc
COPY *.ini ./
COPY *.json ./

ENTRYPOINT [ "./hc" ]
