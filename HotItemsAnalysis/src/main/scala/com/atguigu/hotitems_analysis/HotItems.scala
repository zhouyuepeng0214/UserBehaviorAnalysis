package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间特性为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop110:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

    //    stream.print("input stream")

    stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      //.assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
      override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000
    })
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
      .print("hotitems")


    env.execute("hot items")
  }
}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

//自定义预聚合函数，来一个数据就加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    val itemId: Long = key
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))

  }
}


class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}


//自定义process function，排序处理数据
class TopNHotItems(nSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  //定义一个list state，用来保存所有的ItemViewCount
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemstate", classOf[ItemViewCount]))
  }


  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

    itemState.add(i)

    // 注册定时器，延迟触发；当定时器触发时，当前windowEnd的一组数据应该都到齐，统一排序处理
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 定时器触发时，已经收集到所有数据，首先把所有数据放到一个list中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }
    itemState.clear()

    // todo 从大到小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(nSize)

    // 将数据排名信息格式化成String，方便打印输出
    val result: StringBuilder = new StringBuilder()
    result.append("=====================================\n")
    result.append("时间:").append(new Timestamp(timestamp - 100)).append("\n")

    // todo 集合的下标
    // 每一个商品信息输出
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append("商品ID：").append(currentItem.itemId)
        .append("浏览量：").append(currentItem.count)
        .append("\n")

    }

    result.append("===============================================")

    Thread.sleep(1000)
    out.collect(result.toString())

  }
}
