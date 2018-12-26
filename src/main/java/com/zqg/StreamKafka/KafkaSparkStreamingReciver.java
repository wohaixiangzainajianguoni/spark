package com.zqg.StreamKafka;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class KafkaSparkStreamingReciver {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaSparkStreamingReciver").setMaster("local[2]");
        sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","true");
        sparkConf.set("spark.streaming.concurrentJobs","10");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        javaStreamingContext.sparkContext().setLogLevel("warn");
        javaStreamingContext.checkpoint("d://checkpoint");
        HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();
        stringIntegerHashMap.put("zqg",1);
        JavaPairReceiverInputDStream<String, String> consumergroup = KafkaUtils.createStream(
                javaStreamingContext,
                "127.0.0.1:2181",
                "consumergroup"
                , stringIntegerHashMap
        );
        JavaDStream < String > words = consumergroup.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

                    @Override
                    public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
//                        System.out.println(tuple2._1);
//                        System.out.println(tuple2._2);
                        String[] split = tuple2._2.split("\t");
                        return Arrays.asList(split).iterator();
                    }
                });

       words.mapToPair(new PairFunction<String, String, Integer>() {
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
