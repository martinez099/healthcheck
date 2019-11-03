# healthcheck
This is a command-line tool to check the health of a Redis Enterprise cluster.

## Description
- It does read-only operations via SSH and the Redis Enterprise REST-API.
- You can run single checks or whole check suites.
- Checks may or may not have parameter maps, i.e. JSON files with parameters.

## Prerequisites
- Python 3, no dependencies are required.
- HTTP access to the REST-API of a Redis Enterprise cluster.
- SSH access to all nodes of the Redis Enterprise cluster.

## Configure
- Fill in the `config.ini` in the main directory.
- Alternatively you can pass a different configuration filename with `-cfg <CONFIG>`.

## Run
- To see all available checks, run `healthcheck.py -l`.
- Choose a check suite, run `healthcheck.py -s <SUITE>`, e.g.
  - run `healthcheck.py -s node` for node checks.
- If a suite requires a parameter map, run `healthcheck.py -s <SUITE> -p <PARAMS>`, e.g.
  - run `healthcheck.py -s cluster -p reco` for cluster checks with `recommended` HW requriments.
  - run `healthcheck.py -s cluster -p mini` for cluster checks with `minimum` HW requirements.
  - run `healthcheck.py -s database -p 1` for database checks with parameters given in `config1.json`.
- Alternatively run a single check with `healtchcheck.py -c <CHECK>`, e.g.
  - run `healthcheck.py -c link` to get the network link between the nodes.
- For a quick help, run `healthcheck.py -h`.

## Result
Each check has a result which is indicated by:
- `[+]` check succeeded: The conditions were met.
- `[-]` check failed: The conditions were not met.
- `[~]` check with no result: There were no conditions, this check only output some values.
- `[*]` check had an error: The check could not be executed due to an error.
- `[ ]` check was skipped: The check was omitted because it did not apply in that context.
