#!/bin/bash

read -p "清输入类路径：" mainclass
if [ -z "$mainclass" ]
then
  echo "类路径不能为空，终止脚本运行"
  exit 7
fi
echo "类路径为：$mainclass"

read -p "清输入 Jar 路径：" jar_path
if [ -z "$jar_path" ]
then
  echo "Jar 路径不能为空，终止脚本运行"
  exit 7
fi
echo "Jar 路径为：$jar_path"

flink run --class "$mainclass" "$jar_path"