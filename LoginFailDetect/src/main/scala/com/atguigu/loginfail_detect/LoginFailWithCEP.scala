package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val loginEventStream: KeyedStream[LoginEvent, Long] = env.fromCollection(List(
//      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
//      LoginEvent(1, "192.168.0.3", "success", 1558430846),
//      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
//      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
//      LoginEvent(2, "192.168.10.10", "success", 1558430845)
//    ))
      val loginEventStream: KeyedStream[LoginEvent, Long] = env.socketTextStream("hadoop110",7777)
      .map(data => {
      val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong,dataArray(1),dataArray(2),dataArray(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000
      })
      .keyBy(_.userId)

    // 2. 定义匹配的模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(10))

    // 3. 将pattern应用到输入流上，得到一个pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream,loginFailPattern)

    // 4. 用select方法检出符合模式的事件序列
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print("warning")
    loginEventStream.print("input")


    env.execute()

  }

}

// 自定义pattern select function，输出报警信息
class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  // 从map中可以按照模式的名称提取对应的登录失败事件
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail : LoginEvent = map.get("begin").iterator().next()
    val secondFail : LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,secondFail.eventTime,"在2秒内连续2次登录失败。")
  }
}
