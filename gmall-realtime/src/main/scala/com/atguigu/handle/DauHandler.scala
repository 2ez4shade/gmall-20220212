package com.atguigu.handle

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author: shade
 * @date: 2022/6/21 19:17
 * @description:
 */
object DauHandler {
  /**
   * 分区内过滤
   * @param disfillterDStream
   */
  def filterbyGroup(disfillterDStream: DStream[StartUpLog]) = {

    val mapDStream: DStream[((String, String), StartUpLog)] = disfillterDStream.mapPartitions(_.map(i => ((i.mid, i.logDate), i)))

    val groupDStream: DStream[((String, String), Iterable[StartUpLog])] = mapDStream.groupByKey()

    val mapValuesDStream: DStream[((String, String), List[StartUpLog])] = groupDStream.mapValues(values => {
      values.toList.sortWith(_.ts < _.ts).take(1)
    })

    mapValuesDStream.flatMap(_._2)

  }

  /**
   * 把数据批次间去重
   *
   * @param StartUplogDStream
   */
  def fillterByRedis(StartUplogDStream: DStream[StartUpLog]) = {
    //fillter 过滤连接过多
      /*StartUplogDStream.filter(startUpLog=>{

        val jedis = new Jedis("hadoop102", 6379)
        val key: String = "DAU:" + startUpLog.logDate

        val boolean: lang.Boolean = jedis.sismember(key, startUpLog.mid)

        jedis.close()
        !boolean
      })*/
    //每个分区一个连接
    /*StartUplogDStream.transform(RDD=>{
      RDD.mapPartitions(tmps=>{
        val jedis = new Jedis("hadoop102", 6379)

        val iterator: Iterator[StartUpLog] = tmps.filter(startUpLog => {
          val key: String = "DAU:" + startUpLog.logDate
          val boolean: lang.Boolean = jedis.sismember(key, startUpLog.mid)
          !boolean
        })

        jedis.close()

        iterator
      })
    })*/
    //广播变量
    StartUplogDStream.transform(RDD=>{

      val jedis = new Jedis("hadoop102", 6379)

      //获取系统时间
      val dateformat = new SimpleDateFormat("yyyy-MM-dd")

      val date: String ="DAU:" + dateformat.format(new Date(System.currentTimeMillis()))

      val set: util.Set[String] = jedis.smembers(date)

      RDD.context.broadcast(set)

      val i: RDD[StartUpLog] = RDD.mapPartitions(tmps => {
        tmps.filter(startUpLog => {
          !set.contains(startUpLog.mid)
        })
      })

      jedis.close()

      i
    })
  }

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
          val key: String = "DAU:" + startUpLog.logDate
          //写入数据
          jedis.sadd(key, startUpLog.mid)
        })
        //关闭redies
        jedis.close()
      })
    })
  }

}
