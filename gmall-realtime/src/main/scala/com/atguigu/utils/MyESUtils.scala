package com.atguigu.utils

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

import java.util

/**
 * @author shade
 * @date 2022/6/27 17:04
 * @description
 */
object MyESUtils {

  private val EShost="http://hadoop102"
  private val ESport="9200"
  private var factory:JestClientFactory=null


  /**
   * 创建工厂
   *
   * @return
   */
  def createFactory(): JestClientFactory = {
    val jestClientFactory = new JestClientFactory

    val config: HttpClientConfig = new HttpClientConfig.Builder(EShost + ":" + ESport).build()

    jestClientFactory.setHttpClientConfig(config)
    jestClientFactory
  }

  /**
   * 创建Jest对象
   * @return
   */

  def getClient():JestClient={
    if (factory==null){
      factory=createFactory()
    }
    factory.getObject
  }

  /**
   * 关闭对象
   * @param clien
   */
  def close(clien:JestClient): Unit ={
    if (clien!=null){
      clien.shutdownClient()
    }
  }




  def sendTOES(index:String,list:List[(String,Any)])={
      if (list.nonEmpty){
        val client: JestClient = getClient()

        val indexes = new util.ArrayList[Index]()

        for ((id,source) <- list) {
          val index1: Index = new Index.Builder(source).index(index).`type`("_doc").id(id).build()
          println("增加了一条")
          indexes.add(index1)
        }

        val bulk: Bulk = new Bulk.Builder().addAction(indexes).build()

        client.execute(bulk)

        close(client)
      }
  }


}
