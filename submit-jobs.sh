#!/bin/bash

# 定义一个数组，其中包含了你要提交的所有PySpark脚本的路径
pyspark_scripts=(
"./rating_counts.py"
"./temperature_search.py"
"./customer_spent.py"
"./fridends_by_age.py"
)

# 对数组中的每个元素（即每个PySpark脚本的路径）执行循环
for script in "${pyspark_scripts[@]}"
do
  # 使用spark-submit命令提交PySpark任务
  spark-submit --master local[4] $script
done