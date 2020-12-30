package com.test.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.{Properties, UUID}
import scala.util.Random

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合中读取数据
    val studentList = List(
      Student(3113002614l, "kang", 25),
      Student(3113002615l, "heng", 25),
      Student(3113002616l, "hao", 25),
      Student(3113002617l, "jian", 25),
      Student(3113002618l, "rong", 25)
    )
    val stream1 = env.fromCollection(studentList)
    //    stream1.print()

    //从文件读取数据
    val input: String = "hdfs://hadoop000:9000/data/flink/wc.txt"
    val stream2 = env.readTextFile(input)
    //    stream2.print()

    //从kafka读取数据
    // ./bin/kafka-console-producer.sh --broker-list hadoop000:9092 --topic student
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop000:9092")
    properties.setProperty("group.id", "CID_student")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization,StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization,StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("student", new SimpleStringSchema(), properties))
    //    stream3.print()

    //自定义Source
    val stream4 = env.addSource(new MyStudentFunction())
    stream4.print()


    //执行
    env.execute()
  }

  class MyStudentFunction extends SourceFunction[Student] {

    var flag:Boolean = true
    val rand = new Random()

    override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
      val tmpData = (1 to 10).map(index => (index,s"Robot_${index}",20))

      while(flag){
        val result = tmpData.map(item => Student(item._1,item._2,item._3+rand.nextGaussian().toInt))
        result.foreach(
          data => ctx.collect(data)
        )
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = flag = false
  }

  //定义学生样例类
  case class Student(id: Long, name: String, age: Int)
}


