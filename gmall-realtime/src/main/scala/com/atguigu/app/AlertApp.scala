package com.atguigu.app


import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyESUtils, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}


/**
 * @author: shade
 * @date: 2022/6/27 11:01
 * @description:
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    //获取kafkaDStream
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //定义日期格式后面要用
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    //转化为样例类
    val tupleDStream: DStream[(String, EventLog)] = KafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val log: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val date: String = dateFormat.format(new Date(log.ts))
        log.logDate = date.split(" ")(0)
        log.logHour = date.split(" ")(1)
        (log.mid, log)
      })
    })
    //需要在转化为样例类后  因为window需要类型是可以序列化的
    val fiveDStream: DStream[(String, EventLog)] = tupleDStream.window(Minutes(5))

    //按照mid分组
    val groupbykeyDStream: DStream[(String, Iterable[EventLog])] = fiveDStream.groupByKey()

    //将集合数据合并成 (是否是,警告信息对象)的二元组
    val InfoDStream: DStream[(Boolean, CouponAlertInfo)] = groupbykeyDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>

        val uidsset = new util.HashSet[String]()
        val itemIdsset = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        //肯定不是的标志位
        var bool = true

        breakable{
          for (elem <- iter) {
            eventList.add(elem.evid)
            if ("clickItem".equals(elem.evid)) {
              //如果有预览商品 肯定不是 跳过
              bool = false
              break
            } else if ("coupon".equals(elem.evid)) {
              uidsset.add(elem.uid)
            }
          }
        }

        val info: CouponAlertInfo = new CouponAlertInfo(mid, uidsset, itemIdsset, eventList, System.currentTimeMillis())
        //需要uid个数>=3且没有预览商品
        (info.uids.size() >= 3 && bool, info)
      }
    })

    //取出true的
    val lastInfo: DStream[CouponAlertInfo] = InfoDStream.filter(_._1 == true).map(_._2)

    //增加上传到es时的id
    val ESDStream: DStream[(String, CouponAlertInfo)] = lastInfo.mapPartitions(iter => {
      iter.map(info => {
        (info.mid + (info.ts / 1000 / 60), info)
      })
    })


    //上传到ES
    ESDStream.foreachRDD(rdd=>{
      //es的index id
      val indexname=GmallConstants.ES_ALERT_INDEXNAME+dateFormat.format(new Date(System.currentTimeMillis())).split(" ")(0)

      rdd.foreachPartition(part=>{
        MyESUtils.sendTOES(indexname,part.toList)
      })
    }
    )

    //
    ssc.start()
    ssc.awaitTermination()


  }
}
