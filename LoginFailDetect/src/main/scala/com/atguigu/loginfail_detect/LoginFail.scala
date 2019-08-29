package com.atguigu.loginfail_detect


import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFail {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
//      LoginEvent(1, "192.168.0.3", "success", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000
      })
      .keyBy(_.userId)
      .process(new LoginWarning())
      .print()

    env.execute()

  }

}

case class LoginEvent(userId : Long,ip : String,eventType : String,eventTime : Long)

case class Warning(userId : Long,firstFailTime : Long,lastFailTime : Long,warningMsg : String)

// 自定义处理函数，保存上一次登录失败的事件，并可以注册定时器
class LoginWarning() extends KeyedProcessFunction[Long,LoginEvent,Warning]{

  //定义保存登录失败事件的状态
  lazy val loginFailState : ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailState",classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    if(i.eventType == "fail") {
      // 先获取之前失败的事件
      val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
      // 如果之前已经有失败的事件，就做判断，如果没有就把当前失败事件保存进state
      if (iter.hasNext) {
        val firstFailEvent: LoginEvent = iter.next()
        // 判断两次失败事件间隔小于2秒，输出报警信息
        if(i.eventTime < firstFailEvent.eventTime + 2) {
          collector.collect(Warning(i.userId,firstFailEvent.eventTime,i.eventTime,"在2秒内连续两次登录失败"))
        }
        loginFailState.clear()
        // 把最近一次登录失败保存到state
        loginFailState.add(i)
      } else {
        loginFailState.add(i)
      }
    } else {
      loginFailState.clear()
    }
//    if(i.eventType == "fail") {
//      loginFailState.add(i)
//      context.timerService().registerEventTimeTimer((i.eventTime + 2) * 1000L)
//    } else {
//      loginFailState.clear()
//    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//
//    val allLoginFailEvent : ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//    val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
//    while(iter.hasNext) {
//      allLoginFailEvent += iter.next()
//    }
//    if (allLoginFailEvent.length >=2) {
//      out.collect(Warning(allLoginFailEvent.head.userId,
//        allLoginFailEvent.head.eventTime,
//        allLoginFailEvent.last.eventTime,
//      "在2秒内连续登陆失败" + allLoginFailEvent.length + "次。"))
//    }
//    //清空状态，重新开始计数
//    loginFailState.clear()
//
//  }
}