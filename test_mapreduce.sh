#!/bin/bash
# Run worker scripts in parallel (4 instances)
for i in {1..4}; do
    node worker/worker.mjs &
done

# Wait for all background processes to finish
wait
