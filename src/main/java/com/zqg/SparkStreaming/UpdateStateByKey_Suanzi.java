package com.zqg.SparkStreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Option;
import scala.Tuple2;

import java.util.*;

public class UpdateStateByKey_Suanzi {

    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("updateStateBykey");

        sparkConf.setMaster("local");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        /**
         * checkpoint  的读写的时间，当
         *
         *         JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
         *         Durations.seconds(5) 的值
         *         小于10秒的时候那么checkpoint  每10秒写一次，
         *         当Durations.seconds(5) 的值的值大于现在10 的时候也就是10s 也就是
         *           值设置为多少那么就多少秒读写一次checkpoint
         *
         *
         *
         *
         */
        javaStreamingContext.checkpoint("D:\\checkpoint");


        javaStreamingContext.sparkContext().setLogLevel("warn");

        HashMap<String, String> kafkaBrokers = new HashMap<>();
        kafkaBrokers.put("metadata.broker.list","127.0.0.1:9092");
        HashSet<String> topics = new HashSet<>();
        topics.add("zqg");

        JavaPairInputDStream<String, String> directStream =
                KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaBrokers,
                topics
        );

        JavaDStream<String> stringJavaDStream = directStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {

                String[] s = tuple2._2.split("\t");
                Iterator<String> iterator = Arrays.asList(s).iterator();
                return iterator;
            }
        });
        stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {

            boolean result = true;


            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {


                if (result) {
                    result = false;
                    return new Tuple2<>(s, 1);
                } else {
                    result = true;
                    return new Tuple2<>(s, 2);

                }


            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            /**
             *
             * @param integers   当前批次过来的键相同的数据，
             * @param integerOptional
             * @return
             * @throws Exception
             */
            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> integerOptional) throws Exception {

                Integer sum = 0;
                /**
                 * 如果之前存在数据
                 */
                if (integerOptional.isPresent()) {
                    /**
                     * 获取到历史数据
                     */
                    sum = integerOptional.get();
                }
                for (Integer integer : integers) {
                    sum += integer;
                }
                return Optional.of(sum);
            }
        }).print();

        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
