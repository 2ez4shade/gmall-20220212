package com.atguigu.app



import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
/**
 * @author: shade
 * @date: 2022/6/24 12:21
 * @description:
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //conf
    val conf: SparkConf = new SparkConf().setAppName("master").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val OrderDStream: DStream[OrderInfo] = kafkaDStream.transform(RDD => {
      val OrderInfoRDD: RDD[OrderInfo] = RDD.mapPartitions(partition => {
        partition.map(record => {
          val str: String = record.value()
          val info: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
          info.create_date = info.create_time.split(" ")(0)
          info.create_hour = info.create_time.split(" ")(1).split(":")(0)
          info
        })
      })
      OrderInfoRDD
    })

    OrderDStream.foreachRDD(RDD=>{
      RDD.saveToPhoenix("GMALL2021_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR") ,
        HBaseConfiguration.create(),
        zkUrl = Some("hadoop102:2181"))
    })

//    kafkaDStream.foreachRDD(RDD=>{
//      val OrderInfoRDD: RDD[OrderInfo] = RDD.mapPartitions(partition => {
//        partition.map(record => {
//          val str: String = record.value()
//          val info: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
//          info.create_date=info.create_time.split(" ")(0)
//          info.create_hour=info.create_time.split(" ")(1).split(":")(0)
//          info
//        })
//      })
//      OrderInfoRDD.saveToPhoenix("GMALL2021_ORDER_INFO",
//        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR") ,
//        HBaseConfiguration.create(),
//        zkUrl = Some("hadoop102:2181"))
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
