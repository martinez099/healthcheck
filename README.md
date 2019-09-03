# healthcheck
 a command-line tool to check the health of a Redis Enterprise cluster

## Prerequisites

- Python3
- a Redis Enterprise cluster
- SSH access to all nodes

## Help

healthcheck.py -h

## Run

- chmod u+x healthcheck.py
- ./healthcheck.py $FQDN $USER $PASS martin_redislabs_com 35.240.64.64,35.195.21.231,35.205.201.124 $PATH_TO_KEYFILE
