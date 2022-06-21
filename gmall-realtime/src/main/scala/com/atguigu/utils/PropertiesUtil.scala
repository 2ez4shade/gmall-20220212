package com.atguigu.utils

/**
 * @author: shade
 * @date: 2022/6/21 16:33
 * @description:
 */
import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}

