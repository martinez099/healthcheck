#!/usr/bin/env bash

# run 1 RE container
docker run --detach \
           --cap-add sys_resource \
           --name redislabs \
           --publish 8443:8443 \
           --publish 9443:9443 \
           --publish 12000:12000 \
           redislabs/redis

echo "waiting 60 seconds for Docker container to spin up ..."
sleep 60

# bootstrap the cluster
docker exec --detach \
            --privileged \
            redislabs \
            "/opt/redislabs/bin/rladmin" cluster create name cluster.local username test@redislabs.com password test

echo "waiting 10 seconds for bootstrap process to finish ..."
sleep 10

# create a database
curl --silent \
     --insecure \
     --user "test@redislabs.com:test" \
     --request POST \
     --url "https://localhost:9443/v1/bdbs" \
     --header 'content-type: application/json' \
     --data '{"name":"db1","type":"redis","memory_size":102400,"port":12000}' &> /dev/null

# run SUT
cd ..
echo "running checks ..."
./hc --config tests/unit.ini &> /dev/null
rc=${?}

# cleanup
docker stop redislabs
docker rm redislabs

exit ${rc}
