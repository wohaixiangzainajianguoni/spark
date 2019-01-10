package second.logannalsis

import org.apache.spark.{SparkConf, SparkContext}

object LogScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("log").setMaster("local")
 val context = new SparkContext(conf);
    val file = context.textFile("D:\\BaiduNetdiskDownload\\光环剩余视频\\spark\\17第十七天_Dataframe\\资料\\log.txt")

    val unit = file.map(_.split("#")(6).toInt)

    val reduce = unit.reduce((_+_))

    val count = unit.count()

    val value = reduce/count
    println(value);
    val min = unit.min()

    println(min);
    val max = unit.max()

    println(max);


  }

}
