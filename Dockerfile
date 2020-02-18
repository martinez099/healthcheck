FROM python:3

RUN mkdir -p /app

RUN git clone https://github.com/Redislabs-Solution-Architects/healthcheck.git /app/healthcheck

WORKDIR /app/healthcheck

RUN chmod u+x hc

# replace config.ini with your own configuration
COPY config.ini my_config.ini

CMD [ "./hc", "-cfg", "my_config.ini" ]
