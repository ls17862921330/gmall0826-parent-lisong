package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.StartupLog
import com.atguigu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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

    //过滤掉redis中已经存在的log
    val filterDStream: DStream[StartupLog] = logDStream.transform { //可以使driver端的代码 5s执行一次
      rdd =>
        println("过滤前：" + rdd.count())
        val jedis: Jedis = RedisUtil.getJedisClient
        val midSet: util.Set[String] = jedis.smembers("dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
        jedis.close()
        val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
        val filteredRDD: RDD[StartupLog] = rdd.filter {
          log =>
            !midBC.value.contains(log.mid)
        }
        println("过滤后：" + filteredRDD.count())
        filteredRDD
    }

    //去掉同一批次的重复mid
    val tupleDStream: DStream[(String, StartupLog)] = filterDStream.map(log => (log.mid, log))
    val realFilteredDstream: DStream[StartupLog] = tupleDStream.groupByKey().map {
      case (k, logItr) =>
        val logs: List[StartupLog] = logItr.toList.sortWith {
          (log1, log2) =>
            log1.ts < log2.ts
        }
        logs(0)
    }

    realFilteredDstream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          logItr =>
            val jedis = RedisUtil.getJedisClient
            for (log <- logItr) {
              println(log)
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
