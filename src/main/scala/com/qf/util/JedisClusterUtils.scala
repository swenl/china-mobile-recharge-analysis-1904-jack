package com.qf.util

import java.util

import com.qf.common.Constant
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

/**
  * Description：JedisCluster工具类<br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object JedisClusterUtils {
  private val hostAndPorts = CommonUtil.load.getString(Constant.REDIS_HOST_PORT)

  //准备JedisCluster池，并进行实例的填充
  val pool: util.LinkedList[JedisCluster] = new util.LinkedList

  for (i <- 1 to 20) {
    pool.add(createJedisCluster)
  }


  /**
    * 获得JedisCluster的实例
    *
    * @return
    */
  def createJedisCluster = {
    val timeOut = CommonUtil.load.getInt(Constant.MAX_WAIT_MILLIS)

    val nodes = new util.HashSet[HostAndPort]

    val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(CommonUtil.load.getInt(Constant.MAX_TOTAL))
    poolConfig.setMaxIdle(CommonUtil.load.getInt(Constant.MAX_IDLE))
    poolConfig.setMaxWaitMillis(CommonUtil.load.getInt(Constant.MAX_WAIT_MILLIS))
    poolConfig.setTestOnBorrow(CommonUtil.load.getBoolean(Constant.TEST_ON_BORROW))

    val hosts: Array[String] = hostAndPorts.split("#")

    for (hostport <- hosts) {
      val ipport: Array[String] = hostport.split(":")
      val ip: String = ipport(0)
      val port: Int = ipport(1).toInt
      nodes.add(new HostAndPort(ip, port))
    }

    new JedisCluster(nodes, timeOut, timeOut, 3, "123", poolConfig)
  }


  /**
    * 从池中取出一个JedisCluster的实例
    *
    * @return
    */
  def getJedisCluster = {
    AnyRef.synchronized({
      while (pool.size == 0) {
        CommonUtil.sleep(1)
      }
      pool.poll
    })
  }

  /**
    * 释放资源（本质：将JedisCluster的实例置于pool中，供别的线程使用）
    *
    * @param cluster
    */
  def releaseResource(cluster: JedisCluster) = {
    if (cluster != null) {
      pool.push(cluster)
    }
  }
}
