package com.qf.offline

import org.junit.Test

/**
  * Description：xxxx<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月09日  
  *
  * @author 徐文波
  * @version : 1.0
  */
class TestOffLineCal {
  @Test
  def testZip() = {
    val num1 = List(1, 1, 1)
    val num2 = List(1, 0, 0)

    //List((1,1), (1,0), (1,0))

    val result = num1.zip(num2)
      .map(perEle => perEle._1 + perEle._2)


    //.foreach(println)

    println(result) //List（2,1,1）

    // println(num1.zip(num2))

  }


  @Test
  def testStr = {
    //若是字符串 * 值，字符串出现指定的次数
    println("abc" * 3)
  }
}
