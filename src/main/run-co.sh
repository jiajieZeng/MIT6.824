#!/bin/bash

# 执行go build命令
go build -race -buildmode=plugin ../mrapps/wc.go
go build -race -buildmode=plugin ../mrapps/crash.go

# 删除mr-out*文件
rm mr-out*
rm mr-tmp*

# 执行go run命令
go run -race mrcoordinator.go pg-*.txt
