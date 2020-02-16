package com.atguigu.gmall0826.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {

  /**
   * @Author: lisong
   * @Date: 2020/2/16
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val orderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    val detailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    val orderInfoDStream: DStream[(String, OrderInfo)] = orderDStream.map {
      record =>
        val orderRecord: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderRecord, classOf[OrderInfo])
        val dateSplit: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = dateSplit(0)
        orderInfo.create_hour = dateSplit(1).split(":")(0)

        orderInfo.consignee_tel = orderInfo.consignee_tel.take(3) + "****" + orderInfo.consignee_tel.drop(7)
        (orderInfo.id, orderInfo)
    }
    val orderDetailDStream: DStream[(String, OrderDetail)] = detailDStream.map {
      record =>
        val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (detail.order_id, detail)
    }
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    val saleDetailDStream: DStream[SaleDetail] = joinDStream.flatMap {
      case (orderId, (info, detail)) =>

        val resultList = new ListBuffer[SaleDetail]()
        val jedis: Jedis = RedisUtil.getJedisClient

        if (info != None) {

          val orderInfo: OrderInfo = info.get
          //1. 关联到
          if (detail != None) {
            val saleDetail = new SaleDetail(orderInfo, detail.get)
            resultList += saleDetail
          }

          //2.没关联到
          //2.1 存redis
          val infoKey: String = "order_info:" + orderInfo.id
          jedis.setex(infoKey, 600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

          //2.2 查询redis
          val detailKey: String = "order_detail:" + orderInfo.id
          val detailSet: util.Set[String] = jedis.smembers(detailKey)
          if (detailSet != null && detailSet.size() > 0) {
            import scala.collection.JavaConversions._
            for (elem <- detailSet) {
              val redisDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              resultList += new SaleDetail(orderInfo, redisDetail)
            }
          }

        } else {

          val orderDetail: OrderDetail = detail.get

          //1. 存redis
          val detailKey: String = "order_detail:" + orderDetail.order_id
          jedis.expire(detailKey, 600)
          jedis.sadd(detailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))

          //2. 查询redis
          val infoKey: String = "order_info:" + orderDetail.order_id
          val orderInfoJson: String = jedis.get(infoKey)

          if (orderInfoJson != null && orderInfoJson.length > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            resultList += new SaleDetail(orderInfo, orderDetail)
          }
        }


        jedis.close()
        resultList

    }
    saleDetailDStream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }
  
}
