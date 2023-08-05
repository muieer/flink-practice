package org.muieer.flink_practice.java.function

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple2

object Demo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 程序启动前先从终端使用 netcat 启动输入流：nc -lk 9999
    val socketSource = env.socketTextStream("localhost", 9999)

    socketSource
      .flatMap((line: String, collector: Collector[String]) => {
        val arr = line.split(" ")
        arr.foreach(collector.collect)
      })
      .filter(_.nonEmpty)
      .map(word => Tuple2.of(word, Integer.valueOf(1)))
      .keyBy(_.f0)
      .process(new ProcessWordCount)
      .print()

    env.execute()

  }

}
