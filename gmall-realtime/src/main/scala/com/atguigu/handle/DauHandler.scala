package com.atguigu.handle

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author: shade
 * @date: 2022/6/21 19:17
 * @description:
 */
object DauHandler {
  /**
   * 把数据写入redis
   *
   * @param StartUplogDStream
   */
  def sendToRedis(StartUplogDStream: DStream[StartUpLog]) = {
    StartUplogDStream.foreachRDD(RDD => {
      RDD.foreachPartition(partition => {
        //创建redies连接
        val jedis = new Jedis("hadoop102", 6379)
        partition.foreach(startUpLog => {
          val key: String = "DAU" + startUpLog.logDate
          //写入数据
          jedis.sadd(key, startUpLog.mid)
        })
        //关闭redies
        jedis.close()
      })
    })
  }

}
