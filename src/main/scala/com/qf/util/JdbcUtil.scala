package com.qf.util

import java.sql.{Connection, DriverManager}
import java.util


/**
  * Description：mysql  连接池 <br/>
  * Copyright (c) ， 2018， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object JdbcUtil {
  private val max_connection = 10 //连接池中连接存放总数
  private val connection_num = 10 //产生连接数
  private val pools = new util.LinkedList[Connection] //连接池


  /**
    * 初始化连接池
    */
  for (i <- 1 to max_connection) {
    pools.push(initConn())
  }

  /**
    * 获得连接数
    */
  private def initConn() = {
    Class.forName(CommonUtil.load.getString("db.default.driver"))

    val connection: Connection = DriverManager.getConnection(
      CommonUtil.load.getString("db.default.url"),
      CommonUtil.load.getString("db.default.user"),
      CommonUtil.load.getString("db.default.password")
    )

    connection
  }



  /**
    * 获得连接
    */
  def getConn(): Connection = {
    AnyRef.synchronized({
      while (pools.size == 0) {
        CommonUtil.sleep(1)
      }
      pools.poll() //与此同时，从连接池中删除一个连接的实例
    })
  }

  /**
    * 释放连接
    */
  def releaseCon(con: Connection) {
    pools.push(con)
  }

}
