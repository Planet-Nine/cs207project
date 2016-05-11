#!/bin/bash
trap 'kill $(jobs -p)' EXIT
export PYTHONPATH=.
./go_server_SAX.py &
sleep 1. && ./go_client_SAX.py &
while :
do
  sleep 1
done
