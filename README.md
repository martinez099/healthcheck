# HealthCheck
This is a command-line tool to check the health of a Redis Enterprise cluster.

Click [here](https://docs.google.com/document/d/1yoCBxP40CzRpA525btg8LtlD3yGYSfgZotJw7GIqNrA) for more information.

## Description
The tool does *read-only* operations via remote execution (e.g. SSH) and the Redis Enterprise REST-API.

### Checks
There are 3 categories of checks:
- Configuration
- Status
- Usage

Each check has a result which is indicated by:
- `[+]` check succeeded: The conditions were met.
- `[-]` check failed: The conditions were not met.
- `[~]` check with no result: There were no conditions, this check only output some values.
- `[*]` check had an error: The check could not be executed due to an error.
- `[ ]` check was skipped: The check was omitted because it did not apply in the given context.

### Check Suites
Checks are grouped into check suites, currently there are 3 check suites available:
- Cluster
- Nodes
- Databases
  
### Parameter Maps
Checks may or may not have parameter maps, i.e. JSON files with parameters.
- There are parameter maps given, but you can provide your own.
- To provide your own parameters, clone or edit a paramter map in `parameter_maps/<SUITE>/<CHECK>/`.
- Alternatively you can pass the full filename (i.e. with `.json` at the end) and it will look it up in the current directory.

## Setup
### Prerequisites
- Python 3.7 (no further dependencies required)
- A remote executor:
  - `ssh`
  - `docker`
  - `kubectl`
- Access to a *Redis Enterprise* cluster
  
### Configuration
- Fill in the `config.ini` file with the following configuration data:
  - Under a section call `http`, HTTP access to the REST-API of a Redis Enterprise cluster:
    - Address of the cluster, i.e. FQDN
    - Username of the cluster
    - Password of the cluster
  - Under a scetion calls `ssh`, SSH access to all nodes of the Redis Enterprise cluster:
    - SSH username
    - CSV list of hostnames
    - Path to SSH private key file
  - Alternatively to SSH:
    - Under a section called `docker`, a CSV list of Docker `containers` (name or ID) can be specified.
    - Under a section called `k8s`, a CSV list of Kubernetes `pods` and a `namespace` can be specified.
  - Under a section called `renderer`, a renderer module name can be specified. Options are:
    - `basic` The default renderer.
    - `json` Renders results in JSON format.
    - `html` Renders result in HTML format.
    - `syslog` Renders results according to [RFC5425](https://tools.ietf.org/html/rfc5424) w/o structured data elements.
- Alternatively to `config.ini` you can pass a different configuration filename with `-cfg <CONFIG>`.
- Don't forget to make `hc` executable, e.g. `chmod u+x hc`.

## Run
- To run all check suites, execute `./hc`.
- To see a list of all checks, execute `./hc -l`.
- To run one check suite, execute `./hc -s <SUITE>`, e.g.
  - execute `./hc -s node` for node checks.
- To run a single check, execute `./hc -c <CHECK>`, e.g.
  - execute `./hc -c "network link"` to get the network link between all nodes.
  - execute `./hc -c status` to execute all status check.
  - execute `./hc -c DS-001,DS-002` to execute 1st and 2nd database status check.
- If a check supports a parameter map, execute `./hc -c <CHECK> -p <PARAMS>`, e.g.
  - execute `./hc -c "cluster sizing" -p reco` for cluster sizing check with `recommended` HW requirements.
  - execute `./hc -c "database config" -p config1` for database configuration check with parameter map `config1`.
  - execute `./hc -c "database config" -p my_config.json` for database configuration check with parameters given in `my_config.json` from the current directory.
- For a quick help, execute `./hc -h`.

### Run with Docker
- Either
  - build Docker image and give it a name, e.g. `docker build -t healthcheck:latest .`.
  - pull Docker image form Redis Labs repository, e.g. `docker pull redislabs/healthcheck:latest`.
- Run Docker image with optional arguments, e.g. `docker run healthcheck -s nodes`.

### Return code
The script exits with the following return code:
- 0 - If no errors or failures occured.
- 1 - If preconditions were not met, e.g. wrong parameters passed.
- 2 - If checks failed.
- 3 - If errors occurred.
