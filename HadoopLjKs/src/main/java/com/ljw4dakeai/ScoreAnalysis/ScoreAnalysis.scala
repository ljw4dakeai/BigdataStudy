package com.ljw4dakeai.ScoreAnalysis

import org.apache.spark.{SparkConf, SparkContext}

object ScoreAnalysis {
    def findGrade(score: Int): (Int, String) = score match {
        case x if Math.max(0, score) == Math.min(score, 60) => (score, "不及格")
        case x if Math.max(60, score) == Math.min(score, 80) =>(score, "一般")
        case x if Math.max(80, score) == Math.min(score, 90) =>(score, "良好")
        case x if Math.max(80, score) == Math.min(score, 100) =>(score, "优秀")
    }



    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("ScoreAnalysis")
                .setMaster("local[*]")

        val sc = new SparkContext(conf)

        sc.setLogLevel("ERROR")

        val data = sc
                .textFile("C:\\software\\code\\java\\BigdataStudy\\HadoopLjKs\\src\\main\\java\\com\\ljw4dakeai\\ScoreAnalysis\\score.txt")
                .cache()


//        学号 姓名  math English
//        001   Jerry 81    70

//        1）	统计每个学生的平均成绩（为了防止学生重名，输出平均成绩时同时输出学号，姓名和平均成绩）；
//        2）	统计每门课程的平均成绩（输出课程名和平均成绩）；
//        3）	统计每门课程的最高成绩（输出课程名和最高成绩）；
//        4）    统计每门课程的最低成绩（输出课程名和最低成绩）；
//        5）    统计课程成绩分布情况（包括该课程考试的总人数、90分以上的人数80-89分的人数、60-79分的人数、60分以下的人数）。

        println("各个学生的平均成绩：")
        val analysis1: Unit = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        (data(0) + "-" + data(1), data(2).toInt, (data(3).toInt))
                    }
                )
                .map(value => (value._1, (value._2 + value._3).toFloat / 2))
                .collect()
                .foreach(println)

        println("=========================================================")

        val analysis2 = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        ((data(2).toInt, 1), (data(3).toInt, 1))
                    }
                )
                .reduce(
                    (value1, value2) => {
                        // ((70, 1), (80, 1)) ((80,1), (90, 1))
                        ((value1._1._1 + value2._1._1, value1._1._2 + value2._1._2), (value1._2._1 + value2._2._1, value1._2._2 + value2._2._2))
                    }
                )

        println(s"math的平均成绩为: ${analysis2._1._1/analysis2._1._2} ；\n english的平均成成绩: ${analysis2._2._1/ analysis2._2._2}")
        println("=========================================================")


        val analysis3 = data
                .map(
                    line => {
                        ("math", line.split("\\s+")(2).toInt)
                    }
                ).cache()

        val analysis3_1 = analysis3
                .sortBy(_._2, ascending = true)
                .take(1)

        val analysis3_2 = analysis3
                .sortBy(_._2, ascending = false)
                .take(1)

        println(s"${analysis3_1(0)._1} 的最低成绩为${analysis3_1(0)._2}，最高成绩为${analysis3_2(0)._2}")
        println("=========================================================")

        val analysis4 = data
                .map(
                    line => {
                        ("english", line.split("\\s+")(3).toInt)
                    }
                ).cache()

        val analysis4_1 = analysis4
                .sortBy(_._2, ascending = true)
                .take(1)

        val analysis4_2 = analysis4
                .sortBy(_._2, ascending = false)
                .take(1)


        println(s"${analysis4_1(0)._1} 的最低成绩为${analysis4_1(0)._2}，最低成绩为${analysis4_2(0)._2}")
        println("=========================================================")


        println("math成绩的分布情况")
        val analysis5: Unit = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        findGrade(data(2).toInt)
                    }
                )
                .map(value =>(value._2, 1))
                .reduceByKey{case (x, y) => x+ y}
                .map(value => s"math的${value._1}的人数为${value._2}个")
                .collect()
                .foreach(println)

        println("=========================================================")
        println("english成绩的分布情况")
        val analysis6: Unit = data
                .map(
                    line => {
                        val data = line.split("\\s+")
                        findGrade(data(3).toInt)
                    }
                )
                .map(value =>(value._2, 1))
                .reduceByKey{case (x, y) => x+ y}
                .map(value => s"english的${value._1}的人数为${value._2}个")
                .collect()
                .foreach(println)


        sc.stop()

    }

}
