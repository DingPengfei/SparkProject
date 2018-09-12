/**
  * Created by Administrator on 2017-6-30.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import java.io.File
import java.io.PrintWriter

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    //写成每行逗号后面跟记录号的形式，后面再用split（“，”）分隔
    //    val reader = Source.fromFile("E:\\ECG\\心电文件\\CCDD\\ccdd_labels_HexCode.txt")
    //    val writer = new PrintWriter(new File("E:\\ECG\\心电文件\\CCDD\\sparkTest1.txt"))
    //    for(line <- reader.getLines()) {
    //
    //      val index = line.indexOf("\t")
    //
    //      val num = line.substring(0,index)
    //
    //      val newLine = line.replaceAll(",", ",".concat(num).concat("\t"))
    //
    //      writer.write(newLine.concat("\r\n"))
    //    }
    //
    //    reader.close()
    //    writer.close()

    val conf =
      new SparkConf()
        .setAppName("ClusterScore")
        .setMaster("local") // <--- This is what's missing
        .set("spark.storage.memoryFraction", "1")
    //       .set("spark.driver.memory", "471859200")

    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    //    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
    //    line.flatMap(_.split("\\t")).foreach(println)

    //统计各种病症的记录条数（读取文件为ccdd_labels_HexCode.txt）
    //    line.flatMap(_.split(",")).map(_.replaceAll("[0-9]+[\\t]" , "")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

    //  统计正常的心电记录数量（读取文件为ccdd_labels_HexCode.txt）
    //    println(line.filter(line => line.contains("0x010101,")).count())

    //各病症记录整合汇总（读取文件为sparkTest.txt）
    line.flatMap(_.split(",")).map(x => (x.split("\\t")(0), x)).foreach(println)

    //    line.flatMap(_.replaceAll(",0x[0-9a-z]{6}", ",".concat())
    sc.stop()
  }
}