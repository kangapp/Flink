package com.test.flink

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.Date
import scala.util.Random

object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream:DataStream[Sensor] = env.addSource(new SensorSource)
    val keyedStream:KeyedStream[Sensor,String] = stream.keyBy(_.name)

    /**
     * Keyed State
     * 用valueState保存每个key的上一个温度值，当温差超过 threshold 时，输出报警信息
     * 使用富函数可以获取当前key的状态
     * 两种实现方式：自定义富函数、带状态的函数
     */
    val threshold:Double = 3
    val alarmStream = keyedStream.flatMap(new sensorAlarmFlatMap(threshold))
//    alarmStream.print()

    val alarmStream1 = keyedStream.flatMapWithState[(String,Double,Double),Double]{
      case(sensor:Sensor,None) => ((List.empty),Some(sensor.temperature))
        case(sensor: Sensor,state:Some[Double]) => {
          if((sensor.temperature - state.get).abs > threshold){
            (List((sensor.name,state.get,sensor.temperature)),Some(sensor.temperature))
          } else{
            ((List.empty),Some(sensor.temperature))
          }
        }
    }
//    alarmStream1.print()

    println(env.getStateBackend)

    env.execute()
  }
  case class Sensor(name:String,temperature:Double,timestamp:Long)

  class sensorAlarmFlatMap(threshold:Double) extends RichFlatMapFunction[Sensor,(String,Double,Double)] {

    private var lastTemperature:ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      lastTemperature = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("last_temperature",createTypeInformation[Double])
      )
    }

    override def flatMap(value: Sensor, out: Collector[(String, Double, Double)]): Unit = {
      val tmpCurrentTemperature:Double = lastTemperature.value()
      if(lastTemperature.value != 0 && (value.temperature - tmpCurrentTemperature).abs > threshold){
        out.collect((value.name,tmpCurrentTemperature,value.temperature))
      }
      lastTemperature.update(value.temperature)
    }
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
        Thread.sleep(1000)
      }
    }
    override def cancel(): Unit = flag = false
  }
}
