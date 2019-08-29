package com.atguigu.ordertimeout_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeout {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val orderEventStream: DataStream[OrderEvent] = env.fromCollection(List(
    //      OrderEvent(1, "create", 1558430842),
    //      OrderEvent(2, "create", 1558430843),
    //      OrderEvent(2, "other", 1558430845),
    //      OrderEvent(2, "pay", 1558430850),
    //      OrderEvent(1, "pay", 1558431920)
    //    ))
    val orderEventStream: DataStream[OrderEvent] = env.socketTextStream("hadoop110", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim.toLong)
      })
//      .assignAscendingTimestamps(_.eventTime * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(8)) {
      override def extractTimestamp(t: OrderEvent): Long = t.eventTime * 1000
    })
      .keyBy(_.orderId)
    //定义一个待时间窗口的pattern
    val orderPayPatterm: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("start").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(10))

    // 3. 将pattern应用到输入数据流上
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPatterm)

    //定义一个测输出流的标签
    val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

    //5. 从pattern stream中提取事件序列
    val complexResultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    complexResultStream.print("payed order")
    complexResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout order")

    env.execute("order timeout job")

  }

}

// 定义输入的订单事件样例类
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

// 定义输出的订单检测结果样例类
case class OrderResult(orderId: Long, resultMsg: String)


// 自定义一个 PatternTimeoutFunction，用于处理每一个检测到的超时序列
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId: Long = map.get("start").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")

  }
}

// 自定义一个 PatternSelectFunction，用于处理每一个成功匹配的事件序列
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}

