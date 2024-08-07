#!/usr/bin/env bash

# 捕捉 INT 信号以终止子进程
trap 'kill -INT -$pid; exit 1' INT

# 输出文件
output_file="test_results.txt"

# 清空输出文件（如果存在）
> "$output_file"

# 运行测试
for i in $(seq 1 2); do
    # 使用超时运行测试
    timeout -k 2s 900s time go test -run 2D -race &
    pid=$!
    if ! wait $pid; then
        echo "*** FAILED TESTS IN TRIAL $i" >> "$output_file"
        exit 1
    fi
done

# 所有测试通过时的输出
echo "*** PASSED ALL $i TESTING TRIALS" >> "$output_file"
