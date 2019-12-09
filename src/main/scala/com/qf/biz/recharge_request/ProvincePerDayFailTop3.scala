package com.qf.biz.recharge_request

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.util.JdbcUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description：业务失败省份 TOP3（离线处理[每天]）（存入MySQL） → 源在本地
  * 以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中。
  * <br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object ProvincePerDayFailTop3 {
  def main(args: Array[String]): Unit = {
    // 步骤：
    //①接收离线数据的path,以及特定的日期
    val Array(inputPath, myDate) = Array("file:///C:\\Users\\Administrator\\IdeaProjects\\china-mobile-recharge-analysis-1904-jack\\a_data\\cmcc.json", "20170412")

    //②SparkSession
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // ③将文件中的数据封装成RDD
    val rdd: RDD[String] = sc.textFile(inputPath)

    // ④获取城市信息并封装到广播变量中(若是资源目录下有与hdfs相关的配置文件：core-site.xml，hdfs-site.xml，使用相对路径，默认是hdfs上的资源。若是本地资源，必须使用协议：file:///)
    //val provinceInfo: Map[String, String] = sc.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\china-mobile-recharge-analysis-1904-jack\\a_data\\province.txt")
    val provinceInfo: Map[String, String] = sc.textFile("a_data/province.txt")
      .collect()
      .map(t => {
        val arr = t.split("\\s+")
        (arr(0), arr(1))
      }).toMap
    val provInfoBroadcast = sc.broadcast(provinceInfo)

    //⑤处理业务
    //a)筛选出【充值请求】对应的日志信息
    val rddFilter: RDD[String] = rdd.filter(line => {
      val jsobj: JSONObject = JSON.parseObject(line)
      jsobj.getString("serviceName").equalsIgnoreCase("sendRechargeReq")
    })

    //b)计算,下述RDD中每行数据形如：
    //{"bussinessRst":"0000","channelCode":"6900","chargefee":"20000","clientIp":"112.17.244.230","gateway_id":"WXPAY","interFacRst":"0000","logOutTime":"20170412030032234","orderId":"384663617163048689","payPhoneNo":"15857207376","phoneno":"15857207376","provinceCode":"571","rateoperateid":"","receiveNotifyTime":"20170412030032194","requestId":"20170412030017876364973282669502","retMsg":"接口调用成功","serverIp":"10.255.254.10","serverPort":"8714","serviceName":"payNotifyReq","shouldfee":"20000","srcChannel":"11","sysId":"01"}

    //i)将rdd中每个元素记性变形，形如：每个元素是对偶元组，key: （年月日,省份名）,value: List集合，每个元素形如：1~>条数；充值成功数~>1或是0
    val rddChange: RDD[((String, String), List[Double])] = rddFilter.map(perLine => {
      val jsobj: JSONObject = JSON.parseObject(perLine)
      val result = jsobj.getString("bussinessRst") // 充值结果

      val fee: Double = if (result.equals("0000"))
        jsobj.getDouble("chargefee") else 0.0 // 充值金额

      //val feeCount = if (!fee.equals(0.0)) 1 else 0 // 获取到充值成功数.金额不等于0

      //20170412080506823015869686690289
      val starttime = jsobj.getString("requestId") // 开始充值时间

      val pcode = jsobj.getString("provinceCode") // 获得省份编号
      val province = provInfoBroadcast.value.getOrElse(pcode, "未知") // 通过省份编号进行取值

      // 充值成功数
      val isSucc = if (result.equals("0000")) 1 else 0

      //20170412
      ((starttime.substring(0, 8), province), List[Double](1, isSucc))

    })

    //ii)使用reduduceBykey进行聚合操作，wordcount中的reduceByKey(_+_) <=> reduceByKey((value1,value2)=>value1+value2)
    val resultRDD: RDD[(String, String, Int, String)] = rddChange.reduceByKey((lst1, lst2) => {

      lst1.zip(lst2) //结果形如：List[Double]((1,1),( isSucc, isSucc))
        .map(perTuple => perTuple._1 + perTuple._2)
    }).map(perEle => {
      val time = perEle._1._1
      val province = perEle._1._2
      val totalTimes = perEle._2(0)
      val successTimes = perEle._2(1)

      val failnumber = totalTimes - successTimes //失败次数
      val failrate: Double = failnumber.toDouble / totalTimes
      (time, province, failnumber.toInt, failrate.formatted("%.4f"))
    }).sortBy(_._3, false, 1)


    val finalResult = resultRDD.take(3)

    finalResult.toBuffer.foreach(println)

    save2DB(finalResult)

    //⑥资源释放
    spark.stop
  }


  /**
    * 以省份为维度统计每个省份的充值失败数  按照每天统计一次
    *
    * @param arr
    */
  def save2DB(arr: Array[(String, String, Int, String)]): Unit = {
    //获得Jdbc链接
    val conn = JdbcUtil.getConn()


    //设置时间日期
    arr.foreach(perEle => {

      val sql =
        """
          |insert into tb_failtop3(time,province,failNumber,failRate)
          |values(?,?,?,?)
          |on duplicate key update failNumber=?,failRate=?
        """.stripMargin

      val state = conn.prepareStatement(sql)

      val rateStr = f"${perEle._4.toDouble * 100}%.2f%%"

      state.setString(1, perEle._1)
      state.setString(2, perEle._2)
      state.setInt(3, perEle._3)
      state.setString(4, rateStr)
      state.setInt(5, perEle._3)
      state.setString(6, rateStr)

      state.executeUpdate
    })

    JdbcUtil.releaseCon(conn)
  }
}
