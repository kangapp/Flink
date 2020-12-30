package com.test.flink

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.Date
import scala.util.Random

object TransformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource())

    /**
     * 简单转换算子
     * DataStream → DataStream
     * map()
     * flatmap()
     * filter()
     */
//    stream.filter(item => item.name.equalsIgnoreCase("sensor_1")).print()

    /**
     * DataStream → KeyedStream
     * keyBy()
     *
     * KeyedStream → DataStream
     * reduce()
     * fold()
     * aggregations
     */
//    val keyedStream:KeyedStream[Sensor,String] = stream.keyBy(_.name)
//    keyedStream.minBy("temperature").filter(item => item.name.equalsIgnoreCase("sensor_1")).print()

//    val keyedStream:KeyedStream[(String,Double),String] = stream.map(item => (item.name,item.temperature)).keyBy(_._1)
//    keyedStream.reduce((item1,item2) => (item1._1,item1._2+item2._2)).filter(item => item._1.equalsIgnoreCase("sensor_1")).print()

    /**
     * 合流操作
     * DataStream* → DataStream
     * union()
     *
     * DataStream,DataStream → ConnectedStreams
     * connect()
     * ConnectedStreams → DataStream
     * CoMap, CoFlatMap
     * 模拟场景：温度传感器实时监控温度，同时传入预警温度数据流，当传感器温度大于预警温度时，则发出警报
     */
    val alarmStream = env.addSource(new AlarmSource())
    val connectedStream:ConnectedStreams[Sensor,Double] = stream.connect(alarmStream)
    connectedStream.flatMap(new ConnectMap).filter(item => item.sensor.name.equalsIgnoreCase("sensor_1")).print()

    env.execute()
  }
  case class Sensor(name:String,temperature:Double,timeStamp:Long)
  case class Alert(sensor: Sensor,limit: Double)

  class ConnectMap extends CoFlatMapFunction[Sensor,Double,Alert] {

    private var limit:Double = 38

    override def flatMap1(in1: Sensor, collector: Collector[Alert]): Unit = {
      val temperature = in1.temperature
      if(temperature > limit){
        collector.collect(Alert(in1,limit))
      }
    }

    override def flatMap2(in2: Double, collector: Collector[Alert]): Unit = limit = in2
  }

  class SensorSource() extends SourceFunction[Sensor] {

    var flag = true;
    val random = new Random()

    override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {
      val tmpSource = (1 to 5).map(index => (s"sensor_${index}",37+random.nextDouble()))

      while(flag){
        tmpSource.foreach(item => {
          val sensor:Sensor = Sensor(item._1,item._2+random.nextGaussian(),new Date().getTime)
          sourceContext.collect(sensor)
        })
        Thread.sleep(2000)
      }
    }

    override def cancel(): Unit = flag = false
  }

  class AlarmSource() extends SourceFunction[Double] {

    var flag = true
    val random = new Random()

    override def run(sourceContext: SourceFunction.SourceContext[Double]): Unit = {
      while (flag){
        sourceContext.collect(38 + random.nextGaussian())
        Thread.sleep(10000)
      }
    }

    override def cancel(): Unit = flag = false
  }
}






