package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0826.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  /**
   * @Author: lisong
   * @Date: 2020/2/16
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("alert_app").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val eventDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)

    val windowDStream: DStream[String] = eventDStream.map(_.value()).window(Minutes(5), Seconds(5))

    val mapDStream: DStream[(String, EventInfo)] = windowDStream.map {
      record =>
        val eventInfo: EventInfo = JSON.parseObject(record, classOf[EventInfo])

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateStr: String = sdf.format(new Date(eventInfo.ts))
        eventInfo.logDate = dateStr.split(" ")(0)
        eventInfo.logHour = dateStr.split(" ")(1)

        (eventInfo.mid, eventInfo)
    }

    val groupDStream: DStream[(String, Iterable[EventInfo])] = mapDStream.groupByKey()

    val alertDStream: DStream[(Boolean, AlertInfo)] = groupDStream.map {
      case (mid, eventItr) =>
        val eventList: List[EventInfo] = eventItr.toList

        val userSet = new util.HashSet[String]()
        val itemSet = new util.HashSet[String]()
        val eventsList = new util.ArrayList[String]()
        var ifClickItem = false

        breakable {
          for (event <- eventList) {

            eventsList.add(event.evid)

            if ("coupon".equals(event.evid)) {
              userSet.add(event.uid)
              itemSet.add(event.itemid)
            }

            if ("clickItem".equals(event.evid)) {
              ifClickItem = true
              break()
            }
          }
        }

        val ifAlert: Boolean = userSet.size() >= 3 && !ifClickItem

        (ifAlert, AlertInfo(mid, userSet, itemSet, eventsList, System.currentTimeMillis()))
    }

    val alertInfoDStream: DStream[AlertInfo] = alertDStream.filter(_._1).map(_._2)

    alertInfoDStream.foreachRDD {
      rdd=>
        rdd.foreachPartition {
          alertInfoItr =>
            val sourceList: List[(String, AlertInfo)] = alertInfoItr.toList.map(alert => (alert.mid + alert.ts / 1000 / 60, alert))
            MyEsUtil.insertBulk(sourceList,GmallConstant.ES_INDEX_ALERT)
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
