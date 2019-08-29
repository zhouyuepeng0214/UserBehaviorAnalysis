package com.atguigu.txmatch_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatch {

  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 输入流1：订单事件流
    val orderEventStream: KeyedStream[OrderEvent, String] = env.fromCollection(List(
      OrderEvent(1, "create", "", 1558430842),
      OrderEvent(2, "create", "", 1558430843),
      OrderEvent(1, "pay", "111", 1558430844),
      OrderEvent(2, "pay", "222", 1558430845),
      OrderEvent(3, "create", "", 1558430846),
      OrderEvent(3, "pay", "333", 1558430849)
    ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(1)) {
        override def extractTimestamp(t: OrderEvent): Long = t.eventTime * 1000L
      })
      .filter(_.txId != "")
      .keyBy(_.txId)

      //输入流2：到账事件流
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.fromCollection(List(
      ReceiptEvent("111", "wechat", 1558430847),
      ReceiptEvent("222", "alipay", 1558430848),
      ReceiptEvent("444", "alipay", 1558430850)
    ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.txId)

    val processedSteam: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
      .process(new TxMatchDetection())

    processedSteam.print("matched")
    processedSteam.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedSteam.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")


    env.execute("tx match job")


  }


  class TxMatchDetection() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{

    lazy val payState : ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payState",classOf[OrderEvent]))
    lazy val receiptState : ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipytState",classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt: ReceiptEvent = receiptState.value()
      // pay事件到来，判断有没有对应的receipt事件
      if (receipt != null) {
        // 如果有，正常匹配，输出到主流
        collector.collect(pay,receipt)
        receiptState.clear()
      } else {
        // receipt还没到，把pay存入状态，注册一个定时器，等待receipt
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 3000L)
      }

    }

    override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      // 同样的，考察pay是否已经来过
      if( pay != null ){
        collector.collect( (pay, receipt) )
        payState.clear()
      } else {
        receiptState.update(receipt)
        context.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 3000L )
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      val pay: OrderEvent = payState.value()
      val receipt: ReceiptEvent = receiptState.value()
      // 分两种情况，pay注册的定时器和receipt注册的定时器
      // 如果两个状态都有值，那么正常匹配，这种情况不会出现；如果有一个没有，就说明对应的事件没有到
      if (pay != null) {
        // 如果pay不为空，说明receipt没有到
        ctx.output(unmatchedPays,pay)
      }
      if(receipt != null) {
        // 说明pay没有到
        ctx.output(unmatchedReceipts,receipt)
      }
      payState.clear()
      receiptState.clear()


    }
  }

}

// 定义输入的订单事件样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
// 定义输入的到账事件样例类
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )
