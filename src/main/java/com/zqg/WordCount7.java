package com.zqg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount7 {


    public static void main(String[] args) {

        SparkConf   conf=new SparkConf();
        conf.setAppName("wordcount");
        conf.setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String> file = javaSparkContext.textFile("C:\\Users\\junmei02\\Desktop\\文件\\我的文件\\hive\\spark\\11第十一天\\文档\\hello.txt");

//         new  FlatMapfunction 的泛型表示的是输入的数据类型和输出的数据类型
        JavaRDD<String> wordRdd = file.flatMap(new FlatMapFunction<String, String>() {
            /**
             * 参数表示的一行，
             * @param line
             * @return
             * @throws Exception
             */
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split("\t")).iterator();
            }
            
        });

        /**
         * 第一个参数输入的数据类型  这里就是一个个的参数
         * 第二个参数 输出的键值对的键的数据类型，这是就是String
         * 第三个参数 输出的键值对的值的数据类型   这里是Interger
         */
        JavaPairRDD<String, Integer> wordOneRDD = wordRdd.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        /**
         * 三个参数都表示
         * key相同的时候表示值的类型，
         *
         */
        JavaPairRDD<String, Integer> wordCount = wordOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        /**
         * tuples2的反省分别表示输入的键值对的类型，
         *    后面的两个参数类型分别为输出的参数的类型
         * @param stringIntegerTuple2
         * @return
         * @throws Exception
         */
        JavaPairRDD<Integer, String> reverse = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {


            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> T) throws Exception {


                return new Tuple2<>(T._2, T._1);
            }
        });

        JavaPairRDD<Integer, String> sort = reverse.sortByKey(false);

        sort.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._2+" "+integerStringTuple2._1);
            }
        });

         javaSparkContext.close();

    }



}
