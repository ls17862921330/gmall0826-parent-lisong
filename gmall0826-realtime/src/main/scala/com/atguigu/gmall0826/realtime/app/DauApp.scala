package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.StartupLog
import com.atguigu.gmall0826.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  /**
   * @Author: lisong
   * @Date: 2020/2/7
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //转变结构 添加date和hour
    val logDStream: DStream[StartupLog] = recordDStream.map {
      record =>
        val startupLog: StartupLog = JSON.parseObject(record.value(), classOf[StartupLog])

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val date: String = sdf.format(new Date(startupLog.ts))
        val dateSplits: Array[String] = date.split(" ")

        startupLog.logDate = dateSplits(0)
        startupLog.logHour = dateSplits(1)

        startupLog
    }

    logDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          logItr =>
            val jedis = new Jedis("hadoop102",6379)
            for (log <- logItr) {
              val dauKey: String = "dau:"+log.logDate
              jedis.sadd(dauKey, log.mid)
              jedis.expire(dauKey, 27 * 60 * 60)
            }
            jedis.close()
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
