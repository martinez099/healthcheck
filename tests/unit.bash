#!/bin/bash

# run 1 RE container
docker run --detach \
           --cap-add sys_resource \
           --name redislabs \
           --publish 8443:8443 \
           --publish 9443:9443 \
           --publish 12000:12000 \
           redislabs/redis
sleep 30

# bootstrap the cluster
docker exec --detach \
            --privileged \
            redislabs "/opt/redislabs/bin/rladmin" cluster create name cluster.local username test@redislabs.com password test
sleep 10

# create 1 database
curl --insecure \
     --user "test@redislabs.com:test" \
     --request POST \
     --url "https://localhost:9443/v1/bdbs" \
     --header 'content-type: application/json' \
     --data '{"name":"db1","type":"redis","memory_size":102400,"port":12000}'

# run SUT
cd ..
./hc --config tests/unit.ini
ret=${?}

# cleanup
docker stop redislabs
docker rm redislabs

exit ${ret}
