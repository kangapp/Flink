# Flink
> Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale
- [Flink](#flink)
  - [概述](#概述)
    - [特点](#特点)
  - [官网案例演示](#官网案例演示)
  - [DataStream API](#datastream-api)
    - [官网案例](#官网案例)
    - [Flink程序组成](#flink程序组成)
    - [Environmeng](#environmeng)
    - [Data Source](#data-source)
      - [基于文件](#基于文件)
      - [基于Socket](#基于socket)
      - [基于集合](#基于集合)
      - [自定义Source addSource()](#自定义source-addsource)
    - [Transform](#transform)
      - [Overview](#overview)
      - [Process Function](#process-function)
    - [Sink](#sink)
  - [Window](#window)
    - [大体结构](#大体结构)
    - [生命周期](#生命周期)
    - [Window Assigners](#window-assigners)
      - [使用](#使用)
      - [类型](#类型)
    - [Window Functions](#window-functions)
      - [ReduceFunction](#reducefunction)
      - [AggregateFunction](#aggregatefunction)
      - [ProcessWindowFunction](#processwindowfunction)
      - [ProcessWindowFunction with Incremental Aggregation](#processwindowfunction-with-incremental-aggregation)
      - [Using per-window state in ProcessWindowFunction](#using-per-window-state-in-processwindowfunction)
    - [Triggers](#triggers)
      - [触发器（Trigger）针对不同事件有不同的方法](#触发器trigger针对不同事件有不同的方法)
      - [`TriggerResult`返回的动作](#triggerresult返回的动作)
      - [内置Trigger](#内置trigger)
    - [Evictors](#evictors)
      - [evictor可以在窗口函数调用前后从window移除元素](#evictor可以在窗口函数调用前后从window移除元素)
      - [内置evictors](#内置evictors)
    - [Allowed Lateness](#allowed-lateness)
    - [Side Output](#side-output)
  - [Time](#time)
    - [语义](#语义)
    - [Watermarks](#watermarks)
      - [Watermarks in Parallel Streams](#watermarks-in-parallel-streams)
      - [介绍Watermark Strategies](#介绍watermark-strategies)
      - [Using Watermark Strategies](#using-watermark-strategies)
      - [处理空闲数据源](#处理空闲数据源)
      - [Writing WatermarkGenerators](#writing-watermarkgenerators)
    - [Lateness](#lateness)
    - [Windowing](#windowing)
  - [Table API & SQL](#table-api--sql)
    - [TableEnvironment](#tableenvironment)
      - [用不同的planner创建TableEnvironment](#用不同的planner创建tableenvironment)
    - [Table](#table)
      - [创建表](#创建表)
      - [查询表](#查询表)
    - [集成DataStreamhe和DataSet](#集成datastreamhe和dataset)

## 概述
### 特点
- 事件驱动
- 基于流的世界观
> 有界流和无界流
- 分层API
- 支持事件时间(event-time)和处理时间(processing-time)语义
- exactly-once状态的一致性保证
- 低延迟，每秒处理百万事件，毫秒延迟
- 高可用，动态扩展


## 官网案例演示

- [欺诈检测系统](https://github.com/kangapp/Flink/tree/master/example/FraudDetection)
- [实时报表系统](https://github.com/kangapp/Flink/tree/master/example/Reporting)
- 批处理wordcount
```scala
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
```
- 流处理wordcount
```scala
package com.test.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object StreamingTest {

  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置作业并行度
    env.setParallelism(8)

    //接收一个socket文本流
    val text = env.socketTextStream("localhost",9999)

    //进行转换处理统计
    val resultDataStream: DataStream[(String, Int)] = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(item => item._1)
      .sum(1)

    resultDataStream.print()

    //启动任务执行
    env.execute("Streaming test")
  }
}
```
>7> (world,2)  
4> (hello,2)  
3> (bye,1)  
3> (bye,2)  
3> (he,1)  
3> (he,2)  
4> (hello,3)  
3> (bye,3)  

## DataStream API
### [官网案例](https://github.com/kangapp/Flink/tree/master/example/DataStream)

### Flink程序组成
- Obtain an execution environment
- Load/create the initial data
- Specify transformations on this data
- Specify where to put the results of your computations
- Trigger the program execution
### Environmeng
- `getExecutionEnvironment()`
> 创建一个执行环境，表示当前执行程序的上下文，会根据运行方式返回不同的执行环境：
- createLocalEnvironment()
- createRemoteEnvironment(host: String, port: Int, jarFiles: String*)

### Data Source
#### 基于文件
- `readTextFile(path)`
#### 基于Socket
- `socketTextStream`
#### 基于集合
- `fromCollection() `
#### 自定义Source addSource()
- `SourceFunction` 
- `ParallelSourceFunction`
- `RichParallelSourceFunction`
- connectors
### Transform
#### Overview
- 简单转换算子(DataStream → DataStream)
- 分组(DataStream → KeyedStream)
- 分组聚合(KeyedStream → DataStream)
- 窗口函数(KeyedStream → WindowedStream,DataStream → AllWindowedStream)
- 分流操作(ConnectedStreams → DataStream)
- 合流操作(DataStream,DataStream → ConnectedStreams)
- [更多](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/operators/#datastream-transformations)
#### Process Function
> 低级别流处理操作，可以访问基础的构造块
- events (stream elements)
- state (fault-tolerant, consistent, only on keyed stream)
- timers (event time and processing time, only on keyed stream)
> example
```scala
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

// the source data stream
val stream: DataStream[Tuple2[String, String]] = ...

// apply the process function onto a keyed stream
val result: DataStream[Tuple2[String, Long]] = stream
  .keyBy(_._1)
  .process(new CountWithTimeoutFunction())

/**
  * The data type stored in the state
  */
case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

/**
  * The implementation of the ProcessFunction that maintains the count and timeouts
  */
class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String, Long)] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
    .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


  override def processElement(
      value: (String, String), 
      ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#Context, 
      out: Collector[(String, Long)]): Unit = {

    // initialize or retrieve/update the state
    val current: CountWithTimestamp = state.value match {
      case null =>
        CountWithTimestamp(value._1, 1, ctx.timestamp)
      case CountWithTimestamp(key, count, lastModified) =>
        CountWithTimestamp(key, count + 1, ctx.timestamp)
    }

    // write the state back
    state.update(current)

    // schedule the next timer 60 seconds from the current event time
    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
  }

  override def onTimer(
      timestamp: Long, 
      ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#OnTimerContext, 
      out: Collector[(String, Long)]): Unit = {

    state.value match {
      case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
        out.collect((key, count))
      case _ =>
    }
  }
}
```
### Sink
- `writeAsText()`
- `print()`
- `addSink()`
## Window

### 大体结构
- Keyed Windows
>  Having a keyed stream will allow your windowed computation to be performed in parallel by multiple tasks
```scala
stream
       .keyBy(...)               <-  keyed versus non-keyed windows
       .window(...)              <-  required: "assigner"
      [.trigger(...)]            <-  optional: "trigger" (else default trigger)
      [.evictor(...)]            <-  optional: "evictor" (else no evictor)
      [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
      [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
       .reduce/aggregate/fold/apply()      <-  required: "function"
      [.getSideOutput(...)]      <-  optional: "output tag"
```
- Non-Keyed Windows
> your original stream will not be split into multiple logical streams and all the windowing logic will be performed by a single task
```scala
stream
       .windowAll(...)           <-  required: "assigner"
      [.trigger(...)]            <-  optional: "trigger" (else default trigger)
      [.evictor(...)]            <-  optional: "evictor" (else no evictor)
      [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
      [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
       .reduce/aggregate/fold/apply()      <-  required: "function"
      [.getSideOutput(...)]      <-  optional: "output tag"
```
### 生命周期
- created 
>window is created as soon as the first element that should belong to this window arrives
- removed 
>window is completely removed when the time (event or processing time) passes its end timestamp plus the user-specified allowed lateness
- Trigger
>each window will have a Trigger and a function (ProcessWindowFunction, ReduceFunction, or AggregateFunction) attached to it
- Evictor
>emove elements from the window after the trigger fires and before and/or after the function is applied.

### Window Assigners
> The window assigner defines how elements are assigned to windows
#### 使用
- window(...) (for keyed streams)
- windowAll() (for non-keyed streams)
#### 类型
- tumbling windows
```scala
val input: DataStream[T] = ...

// tumbling event-time windows
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>)

// tumbling processing-time windows
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>)

// daily tumbling event-time windows offset by -8 hours.
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
```
- sliding windows
```scala
val input: DataStream[T] = ...

// sliding event-time windows
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>)

// sliding processing-time windows
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>)

// sliding processing-time windows offset by -8 hours
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
```
- session windows
```scala
val input: DataStream[T] = ...

// event-time session windows with static gap
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>)

// event-time session windows with dynamic gap
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[String] {
      override def extract(element: String): Long = {
        // determine and return session gap
      }
    }))
    .<windowed transformation>(<window function>)

// processing-time session windows with static gap
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>)


// processing-time session windows with dynamic gap
input
    .keyBy(<key selector>)
    .window(DynamicProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[String] {
      override def extract(element: String): Long = {
        // determine and return session gap
      }
    }))
    .<windowed transformation>(<window function>)
```
- global windows
```scala
val input: DataStream[T] = ...

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>)
```
### Window Functions
#### ReduceFunction
> A ReduceFunction specifies how two elements from the input are combined to produce an output element of the same type
```scala
val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
```
#### AggregateFunction
```scala
/**
 * The accumulator is used to keep a running sum and a count. The [getResult] method
 * computes the average.
 */
class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  override def createAccumulator() = (0L, 0L)

  override def add(value: (String, Long), accumulator: (Long, Long)) =
    (accumulator._1 + value._2, accumulator._2 + 1L)

  override def getResult(accumulator: (Long, Long)) = accumulator._1 / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)) =
    (a._1 + b._1, a._2 + b._2)
}

val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .aggregate(new AverageAggregate)
```
#### ProcessWindowFunction
>A ProcessWindowFunction gets an Iterable containing all the elements of the window, and a Context object with access to time and state information
- 概览
```scala
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> implements Function {

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param key The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     *
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    public abstract void process(
            KEY key,
            Context context,
            Iterable<IN> elements,
            Collector<OUT> out) throws Exception;

   	/**
   	 * The context holding window metadata.
   	 */
   	public abstract class Context implements java.io.Serializable {
   	    /**
   	     * Returns the window that is being evaluated.
   	     */
   	    public abstract W window();

   	    /** Returns the current processing time. */
   	    public abstract long currentProcessingTime();

   	    /** Returns the current event-time watermark. */
   	    public abstract long currentWatermark();

   	    /**
   	     * State accessor for per-key and per-window state.
   	     *
   	     * <p><b>NOTE:</b>If you use per-window state you have to ensure that you clean it up
   	     * by implementing {@link ProcessWindowFunction#clear(Context)}.
   	     */
   	    public abstract KeyedStateStore windowState();

   	    /**
   	     * State accessor for per-key global state.
   	     */
   	    public abstract KeyedStateStore globalState();
   	}

}
```
- 使用案例
```scala
val input: DataStream[(String, Long)] = ...

input
  .keyBy(_._1)
  .window(TumblingEventTimeWindows.of(Time.minutes(5)))
  .process(new MyProcessWindowFunction())

/* ... */

class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

  def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[String]) = {
    var count = 0L
    for (in <- input) {
      count = count + 1
    }
    out.collect(s"Window ${context.window} count: $count")
  }
}
```
#### ProcessWindowFunction with Incremental Aggregation
- Incremental Window Aggregation with ReduceFunction
```scala
val input: DataStream[SensorReading] = ...

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .reduce(
    (r1: SensorReading, r2: SensorReading) => { if (r1.value > r2.value) r2 else r1 },
    ( key: String,
      context: ProcessWindowFunction[_, _, _, TimeWindow]#Context,
      minReadings: Iterable[SensorReading],
      out: Collector[(Long, SensorReading)] ) =>
      {
        val min = minReadings.iterator.next()
        out.collect((context.window.getStart, min))
      }
  )
```
- Incremental Window Aggregation with AggregateFunction
```scala
val input: DataStream[(String, Long)] = ...

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .aggregate(new AverageAggregate(), new MyProcessWindowFunction())

// Function definitions

/**
 * The accumulator is used to keep a running sum and a count. The [getResult] method
 * computes the average.
 */
class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  override def createAccumulator() = (0L, 0L)

  override def add(value: (String, Long), accumulator: (Long, Long)) =
    (accumulator._1 + value._2, accumulator._2 + 1L)

  override def getResult(accumulator: (Long, Long)) = accumulator._1 / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)) =
    (a._1 + b._1, a._2 + b._2)
}

class MyProcessWindowFunction extends ProcessWindowFunction[Double, (String, Double), String, TimeWindow] {

  def process(key: String, context: Context, averages: Iterable[Double], out: Collector[(String, Double)]) = {
    val average = averages.iterator.next()
    out.collect((key, average))
  }
}
```
#### Using per-window state in ProcessWindowFunction
> ProcessWindowFunction不仅可以使用keyed state（富函数都可以使用），还可以使用window范围的keyed state，窗口State指向不同的window
- The window that was defined when specifying the windowed operation: This might be tumbling windows of 1 hour or sliding windows of 2 hours that slide by 1 hour.
- An actual instance of a defined window for a given key: This might be time window from 12:00 to 13:00 for user-id xyz. This is based on the window definition and there will be many windows based on the number of keys that the job is currently processing and based on what time slots the events fall into.
> There are two methods on the Context object that a process() invocation receives that allow access to the two types of state:
- globalState(), which allows access to keyed state that is not scoped to a window
- windowState(), which allows access to keyed state that is also scoped to the window

### Triggers
#### 触发器（Trigger）针对不同事件有不同的方法
- The onElement() method is called for each element that is added to a window.
- The onEventTime() method is called when a registered event-time timer fires.
- The onProcessingTime() method is called when a registered processing-time timer fires.
- The onMerge() method is relevant for stateful triggers and merges the states of two triggers when their corresponding windows merge, e.g. when using session windows.
- clear() method performs any action needed upon removal of the corresponding window.
#### `TriggerResult`返回的动作
- CONTINUE: do nothing
- FIRE: trigger the computation
- PURGE: clear the elements in the window
- FIRE_AND_PURGE: trigger the computation and clear the elements in the window afterwards
#### 内置Trigger
- EventTimeTrigger 
- ProcessingTimeTrigger 
- CountTrigger 
- PurgingTrigger 

### Evictors
#### evictor可以在窗口函数调用前后从window移除元素
- evictBefore()
- evictAfter
#### 内置evictors
- CountEvictor
- DeltaEvictor
- TimeEvictor

### Allowed Lateness
> 指定允许元素迟到的时间长度，迟到但未丢失的数据可能会使window重新触发，例如EventTimeTrigger
### Side Output
> 用于配置迟到元素的输出
```scala
val lateOutputTag = OutputTag[T]("late-data")

val input: DataStream[T] = ...

val result = input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .sideOutputLateData(lateOutputTag)
    .<windowed transformation>(<window function>)

val lateStream = result.getSideOutput(lateOutputTag)
```

## Time
### 语义
- Event Time
> In event time, the progress of time depends on the data, not on any wall clocks.Event time programs must specify how to generate Event Time Watermarks, which is the mechanism that signals progress in event time
- Processing Time
> use the system clock of the machines
### Watermarks
> The mechanism in Flink to measure progress in event time is watermarks. Watermarks flow as part of the data stream and carry a timestamp
#### Watermarks in Parallel Streams  
- Watermarks are generated at, or directly after, source functions. Each parallel subtask of a source function usually generates its watermarks `independently`  
- Some operators consume multiple input streams; a union, for example, or operators following a keyBy(…) or partition(…) function. Such an operator’s current event time is the `minimum` of its input streams’ event times
#### 介绍Watermark Strategies
> TimestampAssigner:从数据抽取某些字段的时间戳  
> WatermarkGenerator：告诉系统时间发生的时间
```scala
public interface WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T>{

    /**
     * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this
     * strategy.
     */
    @Override
    TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context);

    /**
     * Instantiates a WatermarkGenerator that generates watermarks according to this strategy.
     */
    @Override
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
}
```
> 可以通过WatermarkStrategy提供的静态方法来创建
```scala
WatermarkStrategy
  .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
  .withTimestampAssigner(new SerializableTimestampAssigner[(Long, String)] {
    override def extractTimestamp(element: (Long, String), recordTimestamp: Long): Long = element._1
  })
```
#### Using Watermark Strategies
> WatermarkStrategy在flink程序中可以在两处地方使用：
- directly on sources
- after non-source operation
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

val stream: DataStream[MyEvent] = env.readFile(
         myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
         FilePathFilter.createDefaultFilter())

val withTimestampsAndWatermarks: DataStream[MyEvent] = stream
        .filter( _.severity == WARNING )
        .assignTimestampsAndWatermarks(<watermark strategy>)

withTimestampsAndWatermarks
        .keyBy( _.getGroup )
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce( (a, b) => a.add(b) )
        .addSink(...)
```
#### 处理空闲数据源
> 当某些分区空闲时，watermark不会更新因为是取所有并行的最小值
```scala
WatermarkStrategy
  .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
  .withIdleness(Duration.ofMinutes(1))
```
#### Writing WatermarkGenerators
```scala
public interface WatermarkGenerator<T> {

    /**
     * Called for every event, allows the watermark generator to examine and remember the
     * event timestamps, or to emit a watermark based on the event itself.
     */
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /**
     * Called periodically, and might emit a new watermark, or not.
     *
     * <p>The interval in which this method is called and Watermarks are generated
     * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
     */
    void onPeriodicEmit(WatermarkOutput output);
}
```
- Writing a Periodic WatermarkGenerator
> watermark产生间隔通过`ExecutionConfig.setAutoWatermarkInterval(...)`来设置
```scala
class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[MyEvent] {

    val maxOutOfOrderness = 3500L // 3.5 seconds

    var currentMaxTimestamp: Long = _

    override def onEvent(element: MyEvent, eventTimestamp: Long): Unit = {
        currentMaxTimestamp = max(eventTimestamp, currentMaxTimestamp)
    }

    override def onPeriodicEmit(): Unit = {
        // emit the watermark as current highest timestamp minus the out-of-orderness bound
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }
}
```
- Writing a Punctuated WatermarkGenerator
> A punctuated watermark generator will observe the stream of events and emit a watermark whenever it sees a special element that carries watermark information.
```scala
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[MyEvent] {

    override def onEvent(element: MyEvent, eventTimestamp: Long): Unit = {
        if (event.hasWatermarkMarker()) {
            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()))
        }
    }

    override def onPeriodicEmit(): Unit = {
        // don't need to do anything because we emit in reaction to events above
    }
}
```
### Lateness
### Windowing
## Table API & SQL

### TableEnvironment
> The TableEnvironment is a central concept of the Table API and SQL integration
- 在内部catalog中注册表
- 注册catalogs
- 加载可插拔的模块
- 执行sql查询
- 注册用户自定义函数
- 将DataStream或DataSet转化为Table
- 持有对`ExecutionEnvironment`或`StreamExecutionEnvironment`的引用

#### 用不同的planner创建TableEnvironment
```scala
// **********************
// FLINK STREAMING QUERY
// **********************
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)
// or val fsTableEnv = TableEnvironment.create(fsSettings)

// ******************
// FLINK BATCH QUERY
// ******************
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

val fbEnv = ExecutionEnvironment.getExecutionEnvironment
val fbTableEnv = BatchTableEnvironment.create(fbEnv)

// **********************
// BLINK STREAMING QUERY
// **********************
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
// or val bsTableEnv = TableEnvironment.create(bsSettings)

// ******************
// BLINK BATCH QUERY
// ******************
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
val bbTableEnv = TableEnvironment.create(bbSettings)
```

### Table
`identifier`
> A TableEnvironment maintains a map of catalogs of tables which are created with an identifier. Each identifier consists of 3 parts: catalog name, database name and object name   
Tables can be either virtual (VIEWS) or regular (TABLES). VIEWS can be created from an existing Table object, usually the result of a Table API or SQL query. TABLES describe external data, such as a file, database table, or message queue.

#### 创建表
- Virtual Tables
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// table is the result of a simple projection query 
val projTable: Table = tableEnv.from("X").select(...)

// register the Table projTable as table "projectedTable"
tableEnv.createTemporaryView("projectedTable", projTable)
```
- Connector Tables
```
tableEnvironment
  .connect(...)
  .withFormat(...)
  .withSchema(...)
  .inAppendMode()
  .createTemporaryTable("MyTable")
```

#### 查询表
- Table API
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register Orders table

// scan registered Orders table
val orders = tableEnv.from("Orders")
// compute revenue for all customers from France
val revenue = orders
  .filter($"cCountry" === "FRANCE")
  .groupBy($"cID", $"cName")
  .select($"cID", $"cName", $"revenue".sum AS "revSum")
```
- SQL
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register Orders table

// compute revenue for all customers from France
val revenue = tableEnv.sqlQuery("""
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)
```

### 集成DataStreamhe和DataSet
- 创建临时视图
```scala
// get TableEnvironment 
// registration of a DataSet is equivalent
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, String)] = ...

// register the DataStream as View "myTable" with fields "f0", "f1"
tableEnv.createTemporaryView("myTable", stream)

// register the DataStream as View "myTable2" with fields "myLong", "myString"
tableEnv.createTemporaryView("myTable2", stream, 'myLong, 'myString)
```
- 转化成表
```scala
// get TableEnvironment
// registration of a DataSet is equivalent
val tableEnv = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, String)] = ...

// convert the DataStream into a Table with default fields "_1", "_2"
val table1: Table = tableEnv.fromDataStream(stream)

// convert the DataStream into a Table with fields "myLong", "myString"
val table2: Table = tableEnv.fromDataStream(stream, $"myLong", $"myString")
```
- 转化成DataStream或DataSet
```scala
// get TableEnvironment. 
// registration of a DataSet is equivalent
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

// Table with two fields (String name, Integer age)
val table: Table = ...

// convert the Table into an append DataStream of Row
val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)

// convert the Table into an append DataStream of Tuple2[String, Int]
val dsTuple: DataStream[(String, Int)] dsTuple = 
  tableEnv.toAppendStream[(String, Int)](table)

// convert the Table into a retract DataStream of Row.
//   A retract stream of type X is a DataStream[(Boolean, X)]. 
//   The boolean field indicates the type of the change. 
//   True is INSERT, false is DELETE.
val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)
```