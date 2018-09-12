/**
  * Created by Administrator on 2017-8-9.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Arrhythmia {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf =
      new SparkConf()
        .setAppName("arrhyrhmia")
        .setMaster("local")
        .set("spark.storage.memoryFraction", "1")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))

    /**
      * 分类统计三级病症记录，读取文件为sparkTest2.txt
      * val C01 = data.map(a => (a.split("\\t")(1), a.split("\\t")(0))).reduceByKey(_+","+_).saveAsTextFile("c:\\data\\classification")
      */


    /**
      * 每条病症单独筛选
      * val C02 = data.filter(_.split("\\t")(1).startsWith("0x02")).saveAsTextFile("c:\\data\\2")
      */


    /**
      * 仅心律失常统计，一级病症。替换病症记录方式，方便下一步放置不同文件夹


    data
      .map(a => a.replaceAll("0x01[0-9a-z]{0,4}", "1"))
      .map(a => a.replaceAll("0x02[0-9a-z]{0,4}", "2"))
      .map(a => a.replaceAll("0x03[0-9a-z]{0,4}", "3"))
      .map(a => a.replaceAll("0x04[0-9a-z]{0,4}", "4"))
      .map(a => a.replaceAll("0x05[0-9a-z]{0,4}", "5"))
      .map(a => a.replaceAll("0x00[0-9a-z]{0,4}", "0"))
      .saveAsTextFile("c:\\data\\replace")
      */

    /**
      * 调换两列顺序，方便批处理。读取文件为replace.txt
      */

    data.map(a => a.split("\\t")(1) + " " + a.split("\\t")(0)).saveAsTextFile("c:\\data\\replace1")
    sc.stop()

//    data.map(line => (line.split("\\s")(1), line.split("\\s")(0)))
  }

}
