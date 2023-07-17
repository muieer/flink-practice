package org.muieer.flink_practice.scala.example

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowWordCountExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*
    * 程序启动前先从终端使用 netcat 启动输入流：nc -lk 9999
    * */
    val socketSource = env.socketTextStream("localhost", 9999)

    val count = socketSource
      .flatMap((line: String, collector: Collector[String]) => {
        val arr = line.split(" ")
        arr.foreach(collector.collect)
      })
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .sum(1)

    count.print()
    env.execute()
  }

}
