package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat

/**
 * @author: shade
 * @date: 2022/6/21 16:36
 * @description:
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //创建配置文件
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //创建sparkstreamcontext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //获取数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val StartUplogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val upLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val date: String = dateFormat.format(upLog.ts)
        upLog.logDate = date.split(" ")(0)
        upLog.logHour = date.split(" ")(1)
        upLog
      }
      )
    })

    StartUplogDStream.cache()

//    StartUplogDStream.print()
    //把数据批次间去重
    val disfillterDStream: DStream[StartUpLog] = DauHandler.fillterByRedis(StartUplogDStream)

//    disfillterDStream.cache()
//
//    disfillterDStream.count().print()

    //对数据分区间去重
    val filterbygroupDStream: DStream[StartUpLog] = DauHandler.filterbyGroup(disfillterDStream)

    filterbygroupDStream.cache()
//
    filterbygroupDStream.count().print()

    //把数据写入redis
    DauHandler.sendToRedis(filterbygroupDStream)


    //数据写入Hbase
    DauHandler.sendToHbase(filterbygroupDStream)

    //

    //启动并阻塞
    ssc.start()
    ssc.awaitTermination()

  }
}
