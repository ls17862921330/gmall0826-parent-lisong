package com.atguigu.gmall0826.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {


  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if ( client!=null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def  insertBulk(sourceList:List[(String ,Any)],indexName:String ): Unit ={

    if(sourceList != null && sourceList.size > 0) {

      val jest: JestClient = getClient

      val bulkBuilder = new Bulk.Builder
      for (elem <- sourceList) {
        println(indexName + elem._2)
        val index: Index = new Index.Builder(elem._2).index(indexName).`type`("_doc").id(elem._1).build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()

      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
      println("共保存了" + items.size() + "条数据")

      close(jest)

    }

  }
}
