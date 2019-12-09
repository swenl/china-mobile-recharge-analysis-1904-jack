package com.qf.biz.recharge_request

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description：以省份为维度,统计每分钟各省的充值笔数和充值金额
  * <br/>
  * Copyright (c) ， 2018， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object ProvincePerMinuteCal {
  def main(args: Array[String]): Unit = {
    // 步骤：
    //①接收离线数据的path,以及特定的日期
    val Array(inputPath) = Array("C:\\Users\\Administrator\\IdeaProjects\\datas\\cmcc.json")

    //②SparkSession
    val spark = SparkSession
      .builder()
      .appName(ProvincePerMinuteCal.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // ③将文件中的数据封装成RDD
    val rdd: RDD[String] = sc.textFile(inputPath)

    // ④获取城市信息并封装到广播变量中
    val provinceInfo: Map[String, String] = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\datas\\province.txt")
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

    //i)将rdd中每个元素记性变形，形如：每个元素是对偶元组，key: （年月日时分,省份名）,value: List集合，每个元素形如：充值成功数~>条数；充值金额~>199.45或是0.0
    val rddChange: RDD[((String, String), List[Double])] = rddFilter.map(perLine => {
      val jsobj: JSONObject = JSON.parseObject(perLine)
      val result = jsobj.getString("bussinessRst") // 充值结果

      val fee: Double = if (result.equals("0000"))
        jsobj.getDouble("chargefee") else 0.0 // 充值金额

      val feeCount = if (!fee.equals(0.0)) 1 else 0 // 获取到充值成功数.金额不等于0

      val starttime = jsobj.getString("requestId") // 开始充值时间

      val pcode = jsobj.getString("provinceCode") // 获得省份编号
      val province = provInfoBroadcast.value.getOrElse(pcode, "未知") // 通过省份编号进行取值

      //201704121429
      ((starttime.substring(0, 12), province), List[Double](feeCount, fee))

    })

    //ii)使用reduduceBykey进行聚合操作，wordcount中的reduceByKey(_+_) <=> reduceByKey((value1,value2)=>value1+value2)
    val resultRDD: RDD[(String, String, Int, String)] = rddChange.reduceByKey((lst1, lst2) => {

      lst1.zip(lst2) //结果形如：List[Double]((1,1),( isSucc, isSucc))
        .map(perTuple => perTuple._1 + perTuple._2)
    }).map(perEle => {
      val time = perEle._1._1
      val province = perEle._1._2
      (time, province, perEle._2(0).toInt, perEle._2(1).formatted("%.2f"))
    }).sortBy(_._2)

    resultRDD.foreach(println)

    //⑥资源释放
    spark.stop
  }
}
