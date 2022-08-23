package com.ljw4dakeai.WeatherAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import scala.language.postfixOps

/**
 * @author zoujiahao
 */
object WeatherAnalysis_ {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
                .setAppName("weatherAnalysis")
                .setMaster("local[*]")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
//        2021 01 01 00    80   -94 10285    50    60     1 -9999 -9999
//        2021 01  01  00   80        -94            10285           50    60         1                      -9999                                 -9999
//        年  月 日 小时 气温(/10) 露点温度(/10) 海平面气压(/10) 风向 风速(/10)  天空条件总覆盖代码 液体沉淀深度维度-一小时间隔(/10) 液体沉淀深度维度-六小时间隔(/10)

        val data = sc
                .textFile("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\china_isd_lite_2021\\*")
                .filter(_.split("\\s+")(4) != "-9999")
                .filter(_.split("\\s+")(5) != "-9999")
                .filter(_.split("\\s+")(6) != "-9999")
                .filter(_.split("\\s+")(7) != "-9999")
                .filter(_.split("\\s+")(8) != "-9999")
                .cache()

        val union_data: Unit = data
                .sortBy(_.split('\t')(0))
                .repartition(1)
                .saveAsTextFile("C:\\software\\code\\java\\HadoopLjKS\\src\\main\\java\\com\\WeatherAnalysis\\allinfo")

//        1.	统计月度平均气温
//        2.	统计每天的平均风速；
//        3.	根据每天平均风速的统计结果，统计各级风力的天数。


        println("月度平均气温")
        val analysis1: Unit = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        (data(1), (data(4).toFloat / 10f, 1))
                    }
                )
                .reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2))
                .map(value => (value._1, value._2._1 / value._2._2))
                .collect()
                .foreach(println)

        println("=========================================================")

        println("每天的平均风速")
        val analysis2: Unit = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        (data(1) + "-" + data(2), (data(8).toFloat / 10f, 1))
                    }
                )
                .reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2))
                .map(value => (value._1, value._2._1 / value._2._2))
                .sortByKey()
                .collect()
                .foreach(println)

        println("=========================================================")

        def findGrade(value: (String, Float)) = value._2 match {
            case x if Math.max(0, x) == Math.min(x, 0.2) => (value._1, "无风")
            case x if Math.max(0.2, x) == Math.min(x, 3.3) => (value._1, "清风")
            case x if Math.max(3.3, x) == Math.min(x, 5.4) => (value._1, "微风")
            case x if Math.max(5.4, x) == Math.min(x, 7.9) => (value._1, "和风")
            case x if Math.max(7.9, x) == Math.min(x, 13.8) => (value._1, "强风")
            case x if Math.max(13.8, x) == Math.min(x, 17.1) => (value._1, "疾风")
            case x if Math.max(17.1, x) == Math.min(x, 21.7) => (value._1, "大风")
            case x if Math.max(21.7, x) == Math.min(x, 24.1) => (value._1, "烈风")
            case x if Math.max(24.1, x) == Math.min(x, 28.4) => (value._1, "狂风")
            case x if Math.max(28.4, x) == Math.min(x, 32.6) => (value._1, "暴风")
            case _ => (value._1, "不确定")
        }

        println("根据每天平均风速的统计结果各级风力的天数:")
        val analysis3: Unit = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        (data(1) + "-" + data(2), (data(8).toFloat / 10f, 1))
                    }
                )
                .reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2))
                .map(value => (value._1, value._2._1 / value._2._2))
                .map(findGrade)
                .map(value => (value._2, 1))
                .reduceByKey { case (x, y) => x + y }
                .collect()
                .foreach(println)
        sc.stop()

    }


}
