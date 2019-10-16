# healthcheck
This is a command-line tool to check the health of a Redis Enterprise cluster.
- It does read-only operations via SSH and the Redis Enterprise REST-API.
- You can run single checks or whole check suites.
- Check suites may or may not have parameter maps.

## Prerequisites

- Python 3
- HTTP access to the REST-API of a Redis Enterprise cluster
- SSH access to all nodes of the Redis Enterprise cluster

## Configure

Fill in the `config.ini` in the main directory.

## Run

- The best way to start is to run `healthcheck.py -l` to see all available check suites.
- Then chose a suite with `healthcheck.py -s <SUITE_NAME>`.
- Or run a single check with `healtchcheck.py -c <CHECK_NAME>`.
- If a suite requires parameters:
  - run it with `healthcheck.py -s <SUITE_NAME> -p <PARAMETER_MAP_NAME>`.
  - you can find available parameter maps by typing `healthcheck.py -l`.
- For a quick help, type `healthcheck.py -h`.

## Result

Each check has a result which is indicated by:

- `[+]` check was successful
- `[-]` check has failed
- `[~]` check has no result
- `[*]` check has an error
- `[ ]` check was skipped
