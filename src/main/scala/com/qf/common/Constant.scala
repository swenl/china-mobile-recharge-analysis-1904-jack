package com.qf.common

/**
  * Description：共通的常量<br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object Constant {

  //redis相关的常量
  /**
    * redis分布式集群中的主机名，端口号
    */
  val REDIS_HOST_PORT = "redis.host.port"


  /**
    * 最大空闲连接数
    */
  val MAX_IDLE = "maxIdle"

  /**
    * 最大连接数
    */
  val MAX_TOTAL = "maxTotal"

  /**
    * 创建连接超时时间
    */
  val MAX_WAIT_MILLIS = "maxWaitMillis"

  /**
    * 获取连接测试是否可用
    */
  val TEST_ON_BORROW = "testOnBorrow"


  //_________________________________________

  //与TimeUtils相关的常量
  val TIME_PATTERN = "time.pattern"
  val TIME_PATTERN2: String = "time.pattern2"
  val TIME_PATTERN3: String = "time.pattern3"
}
