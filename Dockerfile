FROM python:3

RUN mkdir -p /app

WORKDIR /app

COPY healthcheck healthcheck
COPY hc hc
COPY parameter_maps parameter_maps
COPY tests tests
COPY *.ini ./

ENTRYPOINT [ "./hc" ]
