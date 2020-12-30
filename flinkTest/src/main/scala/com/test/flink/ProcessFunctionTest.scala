package com.test.flink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.Date
import scala.util.Random

object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream:DataStream[Sensor] = env.addSource(new SensorSource)

    val lastTime = 5000l
    val resultStream = stream.keyBy(_.name).process(new IncreWarning(lastTime))

    resultStream.print("result")

    env.execute()
  }
  case class Sensor(name:String,temperature:Double,timestamp:Long)

  class IncreWarning(interval: Long) extends KeyedProcessFunction[String,Sensor,String] {

    private var lastTempState:ValueState[Double] = _
    private var curtimerState:ValueState[Long] = _
    private var tempListState:ListState[Double] = _

    override def open(parameters: Configuration): Unit = {
      lastTempState = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("last_temp",createTypeInformation[Double])
      )
      curtimerState = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("curr_timer",createTypeInformation[Long])
      )
      tempListState = getRuntimeContext.getListState(
        new ListStateDescriptor[Double]("temp_list",createTypeInformation[Double])
      )
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
      val list = tempListState.get().toString
      out.collect(s"传感器${ctx.getCurrentKey}的温度值连续${interval/1000}秒上升:${list}")
      curtimerState.clear()
    }

    override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {
      val lastTemp:Double = lastTempState.value()
      val curTimer:Long = curtimerState.value()

      tempListState.add(value.temperature)
      if(curTimer == 0 && value.temperature > lastTemp){
        val ts:Long = ctx.timerService().currentProcessingTime() + interval
        ctx.timerService().registerProcessingTimeTimer(ts)
        curtimerState.update(ts)
      } else if(value.temperature < lastTemp){
        ctx.timerService().deleteProcessingTimeTimer(curTimer)
        curtimerState.clear()
        tempListState.clear()
      }
      lastTempState.update(value.temperature)
    }
  }

  class SensorSource extends SourceFunction[Sensor] {

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
