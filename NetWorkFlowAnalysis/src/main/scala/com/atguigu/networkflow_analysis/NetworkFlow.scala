package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object NetworkFlow {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间特性为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")

    stream.map(data => {
      val dataArray: Array[String] = data.split(" ")

      // todo 转换时间格式 得到毫秒
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp: Long = format.parse(dataArray(3).trim).getTime
      ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
    })
      // todo 处理乱序的数据 延迟的时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .aggregate(new CountAgg(),new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()

    env.execute("network stream job")

  }

}

case class ApacheLogEvent(ip : String,userId : String,eventTime : Long,method : String,url : String)

case class UrlViewCount(url : String,windowEnd : Long,count : Long)


class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{

  override def apply(url: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(url,window.getEnd,input.iterator.next()))
  }
}

class TopNHotUrls(nSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

  lazy val urlState : ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.add(i)
    // todo 定时器触发后会自动删除，若没有触发则需要手动删除
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews : ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()

    while(iter.hasNext) {
      allUrlViews += iter.next()
    }

    urlState.clear()

    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(nSize)
    val result : StringBuilder = new StringBuilder()

    result.append("=========================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (elem <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(elem)

      result.append("No").append(elem + 1).append(":")
        .append("URL=").append(currentUrlView.url)
        .append("流量：").append(currentUrlView.count).append("\n")
    }
    result.append("=========================================\n\n")


    Thread.sleep(100)

    out.collect(result.toString())


  }
}
