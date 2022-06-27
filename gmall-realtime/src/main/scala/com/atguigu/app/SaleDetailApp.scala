package com.atguigu.app


import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConverters._

/**
 * @author: shade
 * @date: 2022/6/27 20:52
 * @description:
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //订单表流
    val orderKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //订单详情表流
    val detailkafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //用户表流
    val userkafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //转化为订单样例类
    val OrderInfoDStream: DStream[(String, OrderInfo)] = orderKafkaDStream.mapPartitions(part => {
      part.map(record => {
        val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        info.create_date=info.create_time.split(" ")(0)
        info.create_hour=info.create_time.split(" ")(1).split(":")(0)
        (info.id, info)
      })
    })
    //转化为订单详情样例类
    val OrderDetailDStream: DStream[(String, OrderDetail)] = detailkafkaDStream.mapPartitions(part => {
      part.map(record => {
        val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (detail.order_id, detail)
      })
    })

    val fulljoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = OrderInfoDStream.fullOuterJoin(OrderDetailDStream)


    //得到双流join后的DStream
    val SaleDetailnoUserDStream: DStream[SaleDetail] = fulljoinDStream.mapPartitions(part => {
      //后面对象转字符串时需要的隐式参数
      implicit val formats = org.json4s.DefaultFormats
      //用来装转化的对象
      val list = new util.ArrayList[SaleDetail]()
      val jedis = new Jedis("hadoop102", 6379)
      part.foreach { case (orderid, (orderopt, detailopt)) =>
        val orderkey = s"order-$orderid"
        val orderdetailkey = s"orderdetail-$orderid"

        //TODO order不为空
        if (orderopt.isDefined) {
          val order: OrderInfo = orderopt.get
          //TODO order也不为空的话 就加到转化的集合中
          if (detailopt.isDefined) {
            val ordrDetail: OrderDetail = detailopt.get
            val detail = new SaleDetail(order, ordrDetail)
            list.add(detail)
          }

          // TODO 将order缓存到redis中
          val orderString: String = Serialization.write(order)
          jedis.set(orderkey, orderString)
          //设置过期时间
          jedis.expire(orderkey, 300)

          //TODO 再从缓存中查看是否有在等待join的orderdetail有的话就遍历set写入集合中
          if (jedis.exists(orderdetailkey)){
            val set: util.Set[String] = jedis.smembers(orderdetailkey)
            //这里是导一个java scala的集合转换
            import scala.collection.JavaConversions._
            set.foreach(str => {
              val orderdetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
              val saleDetail = new SaleDetail(order, orderdetail)
              list.add(saleDetail)
            })
          }

        } else {
          //进到这这说明OrderDetail肯定不为空
          val orderdetail: OrderDetail = detailopt.get
          //先去查缓存
          if (jedis.exists(orderkey)) {
            //有就合并加入集合
            val info: OrderInfo = JSON.parseObject(jedis.get(orderkey), classOf[OrderInfo])
            val detail = new SaleDetail(info, orderdetail)
            list.add(detail)

          } else {
            //没有就加入缓存
            val str: String = Serialization.write(orderdetail)
            jedis.sadd(orderdetailkey, str)
            jedis.expire(orderdetailkey, 300)
          }
        }

      }
      //关闭jedis
      jedis.close()
      list.asScala.toIterator
    })

  //  SaleDetailnoUserDStream.print(100)
    //用户信息写到缓存
    userkafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(part=>{
        val jedis = new Jedis("hadoop102", 6379)
        part.foreach(record=>{
          //本身是josn字符串 转化是为了获取uid放在key中
          val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          val userkey=s"users:${userInfo.id}"
          jedis.set(userkey,record.value())
        })
        jedis.close()
      })
    })

    //join用户信息
    val SaleDetailDStream: DStream[SaleDetail] = SaleDetailnoUserDStream.mapPartitions(part => {

      val jedis = new Jedis("hadoop102", 6379)
      val details = new util.ArrayList[SaleDetail]()

      part.foreach(saledetail=>{
        val str: String = jedis.get(s"users:${saledetail.user_id}")
        val userinfo: UserInfo = JSON.parseObject(str, classOf[UserInfo])
        saledetail.mergeUserInfo(userinfo)
        details.add(saledetail)
      })
      jedis.close()
      details.asScala.toIterator
    })
    SaleDetailDStream.print()
    //写到es


    //
    ssc.start()
    ssc.awaitTermination()
  }
}
