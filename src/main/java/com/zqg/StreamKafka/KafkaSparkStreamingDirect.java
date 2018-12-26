package com.zqg.StreamKafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


public class KafkaSparkStreamingDirect {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("sparkStreamKafka");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        javaStreamingContext.sparkContext().setLogLevel("warn");

        /**
         * kafka集群的节点地址
         */
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("metadata.broker.list","127.0.0.1:9092");

        /**
         * 主题的集合
         */
        HashSet<String> topics = new HashSet<>();

        topics.add("zqg");
        JavaPairInputDStream<String, String> line = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                stringStringHashMap,
                topics
        );


        /**
         * flatmap 将一行拆分为多行
         * 返回迭代器
         */

        JavaDStream<String> stringJavaDStream = line.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {

                String[] split = tuple2._2.split("\t");
                return Arrays.asList(split).iterator();
            }
        });


        stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {

                return  integer+integer2;
            }
        }).print();




        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();


    }
}
