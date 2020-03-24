#!/bin/bash

# run 1 RE container
docker run -d --cap-add sys_resource --name redislabs -p 8443:8443 -p 9443:9443 -p 12000:12000 redislabs/redis
sleep 30

# bootstrap the cluster
docker exec -d --privileged redislabs "/opt/redislabs/bin/rladmin" cluster create name cluster.local username test@redislabs.com password test
sleep 10

# create 1 database
url -k -u "test@redislabs.com:test" --request POST --url "https://localhost:9443/v1/bdbs" --header 'content-type: application/json' --data '{"name":"db1","type":"redis","memory_size":102400,"port":12000}'

# run SUT
cd ..
./hc -cfg tests/unit.ini
ret=${?}

# cleanup
docker stop redislabs
docker rm redislabs

exit ${ret}
