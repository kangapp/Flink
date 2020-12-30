package com.test.flink

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

object TimeTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * 设置当前环境所有流采用事件事件特性
     * 设置基于时间的窗口的水印自动生成的时间间隔
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(300)

    /**
     * 定义延迟数据输出到侧输出流的标签
     */
    val outputTag = OutputTag[Sensor]("window_late_data")

    /**
     * 读取socket的流数据转化成Sensor类
     */
    val stream:DataStream[Sensor] = env.socketTextStream("127.0.0.1",6789).flatMap(new FlatMapFunction[String,Sensor] {
      override def flatMap(value: String, out: Collector[Sensor]): Unit = {
        val words = value.split(",")
        val name = words(0)
        val temperature = words(1).toDouble
        val timestamp = words(2).toLong
        out.collect(Sensor(name,temperature,timestamp))
      }
    })

    /**
     * 采用内部的WatermarkStrategy创建TimestampAssigner和WatermarkGenerator
     * WatermarkGenerator可分为Periodic WatermarkGenerator 和 Punctuated WatermarkGenerator，可自定义水印生成逻辑
     * 定义事件事件和水印生成
     */
    val windowedStream = stream.assignTimestampsAndWatermarks(
      WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
        override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timestamp
      })
    ).keyBy(_.name).window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(2))
        .sideOutputLateData(outputTag)

    /**
     * 使用全窗口函数统计当前窗口温度值最大的数据，窗口关闭时输出
     * 并输出迟到数据
     */
    val resultStream = windowedStream.reduce(
        (sensor1,sensor2) => if(sensor1.temperature>sensor2.temperature) Sensor(sensor1.name,sensor1.temperature,sensor2.timestamp) else sensor2,
        (key,context,elements,collector:Collector[(Long,Sensor)]) =>{
          val sensor:Sensor = elements.iterator.next()
          collector.collect((context.getStart,sensor))
        })
    resultStream.print()
    resultStream.getSideOutput(outputTag).map(sensor => s"迟到数据：温度${sensor.temperature},时间${sensor.timestamp}").print()

    env.execute()
  }

  case class Sensor(name:String,temperature:Double,timestamp:Long)
}
