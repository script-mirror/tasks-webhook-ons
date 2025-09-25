#!/bin/bash
payload="$1"
echo "Payload: ${payload}"
container_name="task-webhook-ons_$(date +%s)"
trap 'echo "Parando container..."; docker stop $container_name; docker rm $container_name; exit' SIGINT
docker run --rm --name $container_name \
    task-webhook-ons:latest "$payload"