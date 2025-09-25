#!/bin/bash
source .env

docker build --no-cache \
    --build-arg GIT_USERNAME=$git_username \
    --build-arg GIT_TOKEN=$git_token \
    -t task-webhook-ons .