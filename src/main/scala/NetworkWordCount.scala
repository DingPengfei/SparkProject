import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ding on 9/11/2018.
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {

      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // create a context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    lines.print()
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
