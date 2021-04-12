# 基于DataStream API实现欺诈检测
> Credit card fraud is a growing concern in the digital age. Criminals steal credit card numbers by running scams or hacking into insecure systems. Stolen numbers are tested by making one or more small purchases, often for a dollar or less. If that works, they then make more significant purchases to get items they can sell or keep for themselves.
## 需求
- the fraud detector should output an alert for any account that makes a small transaction immediately followed by a large one
- Scammers don’t wait long to make their large purchase to reduce the chances their test transaction is noticed. For example, suppose you wanted to set a 1 minute timeout to your fraud detector

## 最终代码
- 主程序
```scala
import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transactions")

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts
      .addSink(new AlertSink)
      .name("send-alerts")

    env.execute("Fraud Detection")
  }
}
```
- 处理函数类
```scala
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  override def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {

    // Get the current state for the current key
    val lastTransactionWasSmall = flagState.value

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        // Output an alert downstream
        val alert = new Alert
        alert.setId(transaction.getAccountId)

        collector.collect(alert)
      }
      // Clean up our state
      cleanUp(context)
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)
      val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE

      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
      out: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }
}
```

## 代码分析
### 主程序
- 设置流处理执行环境
> The execution environment is how you set properties for your Job, create your sources, and finally trigger the execution of the Job.
```scala
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
```
- 创建数据源
> 交易数据(accountId,timestamp,amount)
```scala
val transactions: DataStream[Transaction] = env
  .addSource(new TransactionSource)
  .name("transactions")
```

- 分区事件处理
> keyBy：分割流数据，并行处理不同账户的交易数据  
process：每个分区数据都调用该方法
```scala
val alerts: DataStream[Alert] = transactions
  .keyBy(transaction => transaction.getAccountId)
  .process(new FraudDetector)
  .name("fraud-detector")
```

- 输出结果
```scala
alerts.addSink(new AlertSink)
```

- 运行作业
> Flink applications are built lazily and shipped to the cluster for execution only once fully formed
```scala
env.execute("Fraud Detection")
```
### 处理函数类
- KeyedProcessFunction
> It provides fine-grained control over both state and time
- ValueState
> ValueState is a form of keyed state, meaning it is only available in operators that are applied in a keyed context  
clear value update