# healthcheck
This is a command-line tool to check the health of a Redis Enterprise cluster.

## Description
- It does read-only operations via SSH and the Redis Enterprise REST-API.
- You can run single checks or whole check suites.
- Checks may or may not have parameter maps, i.e. JSON files with parameters.

## Prerequisites
- A POSIX compatible operating system.
- Python 3, no dependencies are required.

## Configure
- Fill in the `config.ini` file with the following configuration data:
  - SSH access to all nodes of the Redis Enterprise cluster.
  - HTTP access to the REST-API of a Redis Enterprise cluster.
- Alternatively you can pass a different configuration filename with `-cfg <CONFIG>`.
- Don't forget to make `hc` executable, e.g. `chmod u+x hc`.

## Run
- To see all available checks, run `./hc -l`.
- Choose a check suite, run `./hc -s <SUITE>`, e.g.
  - run `./hc -s node` for node checks.
- If a suite requires a parameter map, run `./hc -s <SUITE> -p <PARAMS>`, e.g.
  - run `./hc -s cluster -p reco` for cluster checks with `recommended` HW requriments.
  - run `./hc -s database -p 1` for database checks with parameters given in `config1.json`.
  - run `./hc -s database -p config.json` same checks with parameters given in `config.json` from the current directory.
- Alternatively run a single check with `./hc -c <CHECK>`, e.g.
  - run `./hc -c link` to get the network link between the nodes.
- For a quick help, run `./hc -h`.

## Result
Each check has a result which is indicated by:
- `[+]` check succeeded: The conditions were met.
- `[-]` check failed: The conditions were not met.
- `[~]` check with no result: There were no conditions, this check only output some values.
- `[*]` check had an error: The check could not be executed due to an error.
- `[ ]` check was skipped: The check was omitted because it did not apply in that context.
