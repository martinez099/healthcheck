# healthcheck
This is a command-line tool to check the health of a Redis Enterprise cluster.

Click [here](https://docs.google.com/document/d/1C-vlVB8Xcq8GC_cVQNr_K0RGMAVj8cXWg-KOXOj06i4) for more information.

## Description
The tool does *read-only* operations via SSH remote execution and the Redis Enterprise REST-API.

It checks

- configuration
- sizing
- historical usage data

of a cluster, its nodes and databases.

### Checks
Each check has a result which is indicated by:
- `[+]` check succeeded: The conditions were met.
- `[-]` check failed: The conditions were not met.
- `[~]` check with no result: There were no conditions, this check only output some values.
- `[*]` check had an error: The check could not be executed due to an error.
- `[ ]` check was skipped: The check was omitted because it did not apply in the given context.

### Check Suites
Checks are grouped into check suites, there are 3 check suites available:
- Cluster: Checks cluster status, sizing and usage.
- Nodes: Checks nodes setup, configuration and usage.
- Databases: Checks databases sizing, and configuration and usage.
  
### Parameter Maps
Checks may or may not have parameter maps, i.e. JSON files with parameters.
- There are parameter maps given, but you can provide your own.
- To provide your own parameters, clone or edit a paramter map in `parameter_maps/*/`.
- Alternatively you can pass the full filename (i.e. with `.json` at the end) and it will look it up in the current directory.

## Prerequisites
- Python 3, no dependencies are required.

## Setup
- Fill in the `config.ini` file with the following configuration data:
  - SSH access to all nodes of the Redis Enterprise cluster.
  - HTTP access to the REST-API of a Redis Enterprise cluster.
- Alternatively you can pass a different configuration filename with `-cfg <CONFIG>`.
- Don't forget to make `hc` executable, e.g. `chmod u+x hc`.

## Run
- To run all check suites but skipping checks with parameters, execute `./hc`
- To see all available check suites and their associated checks, execute `./hc -l`.
- To run one check suite, execute `./hc -s <SUITE>`, e.g.
  - execute `./hc -s node` for node checks.
- If a suite requires a parameter map, execute `./hc -s <SUITE> -p <PARAMS>`, e.g.
  - execute `./hc -s cluster -p reco` for cluster checks with `recommended` HW requriments.
  - execute `./hc -s database -p 1` for database checks with parameter map `config1`.
  - execute `./hc -s database -p my_config.json` for database checks with parameters given in `my_config.json` from the current directory.
- To run a single check, execute `./hc -c <CHECK>`, e.g.
  - execute `./hc -c link` to get the network link between the nodes.
- For a quick help, execute `./hc -h`.

## Run with Docker
- Build Docker image and give it a name, e.g. `docker build . --tag hc:latest`
- Run Docker image with optional arguments, e.g. `docker run hc -s nodes`
