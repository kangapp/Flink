package com.test.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FlinkTestApp {

  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val input:String = "hdfs://hadoop000:9000/data/flink/wc.txt"
    val inputDateSet: DataSet[String] = env.readTextFile(input)

    //对数据进行统计，得到（word,count）
    inputDateSet.flatMap(_.toLowerCase().split("\t")).map((_,1))
      .groupBy(0) //以第一个元素作为key，进行分组
      .sum(1) //对所有数据的第二个元素求和
      .print()
  }
}
