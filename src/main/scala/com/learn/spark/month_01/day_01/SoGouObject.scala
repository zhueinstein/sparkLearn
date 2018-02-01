package com.learn.spark.month_01.day_01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建者： ZhuWeiFeng 
  * 日期： 2018/2/1
  */
object SoGouObject {
	def main(args: Array[String]): Unit = {
		if(args.isEmpty || args.size < 2){
			System.out.println("Usage:<file1><file2>")
			System.exit(-1)
		}

		val con = new SparkConf().setAppName("SoGouResult").setMaster("Local")
		val sc = new SparkContext(con)
		val rdd1 = sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 6)
		val rdd2 = rdd1.map(x => (x(1), 1)).reduceByKey(_ + _).map(x =>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1))
		rdd2.saveAsTextFile(args(1))
		sc.stop()
	}

}
