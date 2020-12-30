package com.test.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.util.Date
import scala.util.Random

object WindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream:DataStream[Sensor] = env.addSource(new SensorSource)
    val keyedStream:KeyedStream[Sensor,String] = stream.keyBy(_.name)

    /**
     * 滚动窗口
     * 滑动窗口
     * 会话窗口
     * 全局窗口
     */
    val windowedStream_tumbling = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    val windowedStream_sliding = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    val windowedStream_session = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
    val windowedStream_global = keyedStream.window(GlobalWindows.create())

    /**
     * 增量聚合函数
     * ReduceFunction: reduce()
     * AggregateFunction: min()、minBy()、AggregateFunction
     */
    val reduceResult = windowedStream_tumbling.reduce((sensor1,sensor2) =>
      Sensor(sensor1.name,sensor1.temperature+sensor2.temperature,sensor1.timestamp.max(sensor2.timestamp)))

    val aggregateResult = windowedStream_tumbling.aggregate(new AverageAggregate_sensor)

//    reduceResult.print()
//    aggregateResult.print()

    /**
     * 全窗口函数：ProcessWindowFunction
     */
    val processResult = windowedStream_tumbling.process(new CountProcessWindowFunction)
//    processResult.print()

    /**
     * 全窗口函数和增量聚合函数结合
     * reduce()
     * aggregate()
     */
    val reduceProcessStream = windowedStream_tumbling.reduce(
      (sensor1,sensor2) =>{if(sensor1.temperature > sensor2.temperature) sensor1 else sensor2},
      (key,context,elements,collector:Collector[(Long,Sensor)]) =>{
        val sensor:Sensor = elements.iterator.next()
        collector.collect((context.getStart,sensor))
      })
//    reduceProcessStream.print()

    val aggregateProcessStream = windowedStream_tumbling.aggregate(new AverageAggregate,new AvgProcessWindowFunction)
    aggregateProcessStream.print()

    env.execute()
  }
  case class Sensor(name:String,temperature:Double,timestamp:Long)

  class AvgProcessWindowFunction extends ProcessWindowFunction[Double,(String,Double),String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[(String, Double)]): Unit = {
      val average = elements.iterator.next()
      out.collect((key,average))
    }
  }

  class AverageAggregate extends AggregateFunction[Sensor, (Double, Long), Double] {
    override def createAccumulator() = (0L, 0L)

    override def add(value: Sensor, accumulator: (Double, Long)) =
      (accumulator._1 + value.temperature, accumulator._2 + 1L)

    override def getResult(accumulator: (Double, Long)) = accumulator._1 / accumulator._2

    override def merge(a: (Double, Long), b: (Double, Long)) =
      (a._1 + b._1, a._2 + b._2)
  }

  class CountProcessWindowFunction extends ProcessWindowFunction[Sensor,(String,Long),String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Sensor], out: Collector[(String, Long)]): Unit = {
      var count = 0l
      for(item <- elements){
        count = count + 1l
      }
      out.collect((key,count))
    }
  }

  class AverageAggregate_sensor extends AggregateFunction[Sensor,(String,Double,Long),(String,Double)] {
    override def createAccumulator(): (String, Double, Long) = ("",0.0,0l)

    override def add(value: Sensor, accumulator: (String, Double, Long)): (String, Double, Long) = {
      val name = value.name
      (name,accumulator._2 + value.temperature,accumulator._3 + 1l)
    }

    override def getResult(accumulator: (String, Double, Long)): (String, Double) = (accumulator._1,accumulator._2/accumulator._3)

    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = (a._1,a._2+b._2,a._3+b._3)
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
}
