#!/bin/bash

function notify() {
    echo -e "\\033[35m"$1"\\033[0m" 1>&2
}

while ! sbin/start_cli.sh -h $IGINX_HOST -p $IGINX_PORT -e "clear data;" | tee /dev/stderr | grep -q "success"; do
    sleep 1
    notify "retrying to clear data"
done

notify "successfully cleared data"

notify "Loading Schema Pile"
java -Diginx.host=$IGINX_HOST -Diginx.port=$IGINX_PORT -jar schemapile-loader.jar
notify "Schema Pile loaded"

notify "Registering the transform job"
sbin/start_cli.sh -h $IGINX_HOST -p $IGINX_PORT -e "COMMIT TRANSFORM JOB 'ScheduledEmbedding.yml';" | tee /dev/stderr > job.out
notify "Transform job registered"

jobid=$(grep -oP "job id: \K[0-9]+" job.out)
notify "Job ID: $jobid"

while ! sbin/start_cli.sh -h $IGINX_HOST -p $IGINX_PORT -e "SHOW TRANSFORM JOB STATUS $jobid;" | tee /dev/stderr | grep -q "JOB_IDLE"; do
    sleep 5
    notify "waiting for transform job to finish its first run"
done

notify "Transform job finished its first run"
notify "Successfully initialized the system"
