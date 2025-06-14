#!/bin/bash

# Run client scripts in parallel (5 instances)
for i in {1..5}; do
    node client/client.mjs &
done

# Run worker scripts in parallel (3 instances)
for i in {1..3}; do
    node worker/worker.mjs &
done

# Wait for all background processes to finish
wait
