package com.zqg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;


import java.util.Arrays;

public class WordCount8 {
    public static void main(String[] args) {


        SparkConf sparkConf=new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("WordCountjava8");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
  sc.textFile("C:\\Users\\junmei02\\Desktop\\文件\\我的文件\\hive\\spark\\11第十一天\\文档\\hello.txt")
                .flatMap(line -> Arrays.asList(line.split("\t")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((m, n) -> m + n)
                .mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
                .sortByKey(false)
                .foreach(  t -> {
                    System.out.println(t._2()+" "+t._1);
                });



    }
}
