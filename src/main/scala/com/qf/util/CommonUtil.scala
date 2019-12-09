package com.qf.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Description：共通工具类，使用typesafe库，读取资源目录下资源文件中的内容<br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object CommonUtil {
  /**
    * 加载配置文件
    */
  val load: Config = ConfigFactory.load


  /**
    * 睡眠到指定的秒钟
    *
    * @param second
    */
  def sleep(second: Int) = {
    Thread.sleep(second * 1000)
  }
}
