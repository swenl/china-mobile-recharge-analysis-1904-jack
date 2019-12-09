package com.qf.util

import java.text.SimpleDateFormat


/**
  * Description：时间解析 工具类<br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
object TimeUtils {
  /**
    * 计算每次充值耗费的时间
    *
    * @param starttime
    * @param recivetime
    * @return
    */
  def costtime(starttime: String, recivetime: String): Long = {
    //20170412030109773
    //yyyyMMddHHmmssSSS
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")

    //20170412030103976913602608416201 →流水号
    val startTime = format.parse(starttime.substring(0, 17)).getTime
    val endTime = format.parse(recivetime).getTime
    endTime - startTime
  }
}
