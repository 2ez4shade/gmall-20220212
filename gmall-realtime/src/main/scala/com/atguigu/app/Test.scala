package com.atguigu.app

import com.atguigu.bean.OrderInfo
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization
/**
 * @author: shade
 * @date: 2022/6/27 22:47
 * @description:
 */
object Test {
  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("hadoop102", 6379)
    implicit val formats = org.json4s.DefaultFormats
    val info = new OrderInfo("1", "2", "3"
      , "4", "5", "3"
      , "4", "5"
      , "3", 1, "5"
      , "3", "4", "5"
      , "3", "4", "5"
      , "3", "4", "5")
    val str: String = Serialization.write(info)(formats)
    jedis.set("xxx",str)

    jedis.close()

  }
}
