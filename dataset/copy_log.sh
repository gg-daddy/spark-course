#!/bin/bash

# 清空 logs 目录下的文件
rm -rf logs/*

# 设置复制次数
copy_times=5
# 循环复制文件
for ((i=1; i<=$copy_times; i++))
do
    cp access_log.txt logs/access_log_$i.txt
    echo "Copied access_log_$i.txt at $(date +"%Y-%m-%d %H:%M:%S")"
    sleep 15
done