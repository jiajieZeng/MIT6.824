#!/usr/bin/env bash

trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel

for i in $(seq 1 1000); do
    # timeout -k 2s 900s time go test -run 2D -race &
    timeout -k 2s 900s time go test -run TestSnapshotInstall2D -race &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS
