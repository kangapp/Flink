# Flink
> Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale

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


## 案例演示
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

## 流处理API
### Environmeng
- `getExecutionEnvironment()`
> 创建一个执行环境，表示当前执行程序的上下文，会根据运行方式返回不同的执行环境：
- createLocalEnvironment()
- createRemoteEnvironment(host: String, port: Int, jarFiles: String*)

### Source
- 从集合读取数据
- 从文件读取数据
- 从kafka读取数据
- 自定义Source
### Transform
- 简单转换算子
- 简单分组聚合
- reduce聚合
- 分流操作
- 合流操作
### Sink

## Window

## Time
### 语义
- Event Time
- Processing Time

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