package com.atguigu.ordertimeout_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithStatePro {

  val orderWaningOutputTag = new OutputTag[OrderResult]("orderWarning")

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
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("hadoop110", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      //      .assignAscendingTimestamps(_.eventTime * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(8)) {
      override def extractTimestamp(t: OrderEvent): Long = t.eventTime * 1000
    })
      .keyBy(_.orderId)

    val orderResultStream: DataStream[OrderResult] = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderWaningOutputTag).print("timeout")

    env.execute("order timeout with state job")

  }


  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    // 定义状态，是否已经支付，和定时器时间戳
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))

    override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {

      //先拿到当前状态
      val isPayed: Boolean = isPayedState.value()
      val timerTs: Long = timerState.value()

      // 根据事件的type判断，主要处理create和pay
      if (i.eventType == "create") {
        // 判断是否已经支付过
        if (isPayed) {
          // 1. 如果已经pay过，匹配成功，输出正常支付的信息
          collector.collect(OrderResult(i.orderId, "payed successfully"))
          context.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 2. 如果还没有pay过，注册定时器等待pay事件
          context.timerService().registerEventTimeTimer(i.eventTime * 1000L + 600 * 1000L)
          timerState.update(i.eventTime * 1000L + 600 * 1000L)
        }
      } else if (i.eventType == "pay") {
        // 如果是pay事件，判断是否已经定义了定时器
        if (timerTs > 0) {
          // 3. 有定时器注册，说明有create来过，可以输出结果，定时器时间与事件pay时间做比较
          if (i.eventTime * 1000L < timerTs) {
            // 如果pay的时间小于定时器触发时间，说明没有超时，正常输出
            collector.collect(OrderResult(i.orderId, "payed successfully"))
          } else {
            // 如果大于定时器触发时间，应该报超时
            context.output(orderWaningOutputTag, OrderResult(i.orderId, "payed but alrealdy timeout"))
          }
          // 无论是否超时，都已经处理完毕，情况状态
          context.timerService().deleteEventTimeTimer( timerTs )
          isPayedState.clear()
          timerState.clear()
        } else {
          // 4. 如果没有定时器定义，需要等待create事件到来
          isPayedState.update(true)
          // 可选，可以注册定时器，等待一段时间，如果create不来，也触发一个报警信息
          context.timerService().registerEventTimeTimer(i.eventTime * 1000L)
          timerState.update(i.eventTime * 1000L)

        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

      // 两种触发定时器的情形，一种是create注册了定时器等待pay但pay没来；另一个种是pay来了create没来
      val isPayed: Boolean = isPayedState.value()
      if (isPayed) {
        // 1. create没来但pay来了，说明数据丢失或者业务异常
        ctx.output(orderWaningOutputTag,OrderResult(ctx.getCurrentKey,"alrealdy payed but create log not found"))
      } else {
        // 2. create来了但是没有pay，真正的超时
        ctx.output(orderWaningOutputTag, OrderResult( ctx.getCurrentKey, " order timeout." ))
      }

      isPayedState.clear()
      timerState.clear()

    }
  }

}