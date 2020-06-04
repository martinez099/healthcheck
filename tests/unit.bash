#!/usr/bin/env bash

which ./hc > /dev/null || (echo "./hc not found or not executable"; exit 123)

echo "starting 1 Redis Enterprise Docker container ..."
docker run --detach \
           --cap-add sys_resource \
           --name redislabs \
           --publish 8443:8443 \
           --publish 9443:9443 \
           --publish 12000:12000 \
           redislabs/redis &> /dev/null

echo "waiting 60 seconds for Docker container to spin up ..."
sleep 60

docker exec --detach \
            --privileged \
            redislabs \
            "/opt/redislabs/bin/rladmin" cluster create name cluster.local username test@redislabs.com password test

echo "waiting 10 seconds for bootstrap process to finish ..."
sleep 10

curl --silent \
     --insecure \
     --user "test@redislabs.com:test" \
     --request POST \
     --url "https://localhost:9443/v1/bdbs" \
     --header 'content-type: application/json' \
     --data '{"name":"db1","type":"redis","memory_size":102400,"port":12000}' &> /dev/null

echo "waiting 10 seconds for database to be created ..."
sleep 10

echo "putting some traffic, this takes about 60 seconds ..."
docker exec redislabs "/opt/redislabs/bin/memtier_benchmark" \
            --server localhost \
            --port 12000 \
            --hide-histogram &> /dev/null

echo "running checks, this takes more than 60 seconds ..."
./hc --config tests/unit.ini &> /dev/null
rc=${?}

echo "cleaning up ..."
docker stop redislabs &> /dev/null
docker rm redislabs &> /dev/null

exit ${rc}
