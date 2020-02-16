package com.atguigu.gmall0826.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.OrderInfo
import com.atguigu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.Jedis

object OrderApp {

  /**
   * @Author: lisong
   * @Date: 2020/2/12
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)



    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.transform {
          rdd=>
            val jedis: Jedis = RedisUtil.getJedisClient
            val userSet: util.Set[String] = jedis.smembers("order_user_list")

            val userBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(userSet)

            rdd.map {
              record =>
                val newUserList = new util.ArrayList[String]()

                val orderLog: String = record.value()
                val order: OrderInfo = JSON.parseObject(orderLog, classOf[OrderInfo])
                val dateSplit: Array[String] = order.create_time.split(" ")
                order.create_date = dateSplit(0)
                order.create_hour = dateSplit(1).split(":")(0)

                order.consignee_tel = order.consignee_tel.take(3) + "****" + order.consignee_tel.drop(7)

                if(!userBC.value.contains(order.user_id) && !newUserList.contains(order.user_id)) {
                  order.is_first_order = "1"
                  newUserList.add(order.user_id)
                } else {
                  order.is_first_order = "0"
                }
                println("++++++++++++++++++++++++++++"+ newUserList.size)
                order
            }
    }

    orderInfoDStream.foreachRDD {
      rdd =>
        rdd.saveToPhoenix("GMALL2020_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT"
          , "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL",
          "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO",
          "PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR", "IS_FIRST_ORDER")
          ,new Configuration
          ,Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    orderInfoDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          orderItr =>
            val jedis: Jedis = RedisUtil.getJedisClient
            val redisKey = "order_user_list"
            for (elem <- orderItr.toList) {
              jedis.sadd(redisKey, elem.user_id)
            }
            jedis.close()
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
