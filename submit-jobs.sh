#!/bin/bash


# 指定要搜索的目录
directory="."

# 使用 find 命令查找所有的 Py 文件
pyspark_scripts=$(find $directory -maxdepth 1 -name "*.py")

# 对数组中的每个元素（即每个PySpark脚本的路径）执行循环
for script in $pyspark_scripts
do
  # 使用spark-submit命令提交PySpark任务
  echo "Submitting PySpark job: $script :"
  
  # 记录开始时间
  start_time=$(date +%s)
  
  spark-submit --master local[4] $script
  
  # 记录结束时间
  end_time=$(date +%s)
  
  # 计算并打印运行时间
  duration=$((end_time - start_time))
  echo "Time taken by $script is $duration seconds."
done