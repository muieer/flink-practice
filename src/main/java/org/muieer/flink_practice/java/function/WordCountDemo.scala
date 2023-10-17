package org.muieer.flink_practice.java.function

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple2
import org.muieer.flink_practice.java.config.CommandLineArgumentToFlinkConfiguration
import org.slf4j.{Logger, LoggerFactory}

object WordCountDemo {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val originCommandLineArgument = args.toList.mkString(" ")
    val configuration = CommandLineArgumentToFlinkConfiguration.commandLineArgumentToFlinkConfiguration(originCommandLineArgument)
    log.info(s"parse input command line res: ${configuration.toMap}")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(configuration)

    // 程序启动前先从终端使用 netcat 启动输入流：nc -lk 9999
    env.socketTextStream("localhost", 9999)
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
