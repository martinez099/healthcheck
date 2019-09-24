# healthcheck
This is a command-line tool to check the health of a Redis Enterprise cluster.
It does read-only operations via SSH and the Redis Enterprise REST-API.

## Prerequisites

- Python 3
- HTTP access to the REST-API of a Redis Enterprise cluster
- SSH access to all nodes of the Redis Enterprise cluster

## Configure

Fill in the `config.ini` in the main directory.

## Run

### Help

`healthcheck.py -h`

### List all check suites

`healthcheck.py -l`

### Run a check suite

`healthcheck.py -s <SUITE_NAME>`

### Run a single check

`healtchcheck.py -s <SUITE_NAME> -c <CHECK_NAME>`

### Run a check suite with parameters

`healthcheck.py -s <SUITE_NAME> -p <PARAMETER_MAP_NAME>`

## Result

- `[+]` check satisfied
- `[-]` check not satisfied
- `[*]` check failed with error
- `[~]` check w/o result
- `[ ]` check skipped
