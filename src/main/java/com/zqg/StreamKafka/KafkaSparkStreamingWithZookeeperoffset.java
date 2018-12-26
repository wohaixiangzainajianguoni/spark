package com.zqg.StreamKafka;

import com.zqg.StreamKafka.getOffset.GetTopicOffsetFromKafkaBroker;
import com.zqg.StreamKafka.getOffset.GetTopicOffsetFromZookeeper;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class KafkaSparkStreamingWithZookeeperoffset {

    static  final Logger logger= Logger.getLogger(KafkaSparkStreamingWithZookeeperoffset.class);

    public static void main(String[] args) {


        ProjectUtil.LoadLogConfig();



        /**
         * 从kafka 中获取每个分期生产者生产数据的最大的偏移量
         */
        Map<TopicAndPartition, Long> zqg = GetTopicOffsetFromKafkaBroker.getTopicOffsets("127.0.0.1:9092", "www");

        /**
         * 从zookeeper 中获取一个组从一个分区中消费的offset
         */

        Map<TopicAndPartition, Long> consumerOffsets = GetTopicOffsetFromZookeeper.getConsumerOffsets("127.0.0.1:2181", "consumergroup", "www");


        Set<Map.Entry<TopicAndPartition, Long>> entries =
                consumerOffsets.entrySet();
        for (Map.Entry<TopicAndPartition, Long> entry:entries)
        {
            TopicAndPartition key = entry.getKey();
            Long value = entry.getValue();
            System.out.println(key.topic()+"——"+value);

        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(null!=consumerOffsets && consumerOffsets.size()>0){
            zqg.putAll(consumerOffsets);
        }
//
//        JavaStreamingContext jsc = SparkStreamingDirect.getStreamingContext(zqg,"group1");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("zookeepManageOffsetReloadData");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));




        /**
         * kafka集群的节点地址
         */
        HashMap<String, String> brokerMap = new HashMap<>();
        brokerMap.put("metadata.broker.list","127.0.0.1:9092");

        /**
         * 主题的集合
         */
        HashSet<String> topicSet = new HashSet<>();

        topicSet.add("www");
        JavaInputDStream<String> line = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                brokerMap,
                consumerOffsets,
                new Function<MessageAndMetadata<String, String>, String>() {
                    @Override
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );

        JavaDStream<String> transform = line.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

            @Override
            public JavaRDD<String> call(JavaRDD<String> stringJavaRDD) throws Exception {

                stringJavaRDD.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {

                        System.out.println(s);
                    }
                });
                return stringJavaRDD;
            }
        });

        JavaDStream<String> stringJavaDStream = transform.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split("\t");
                Iterator<String> iterator = Arrays.asList(split).iterator();
                return iterator;
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>(s, 1);
                return stringIntegerTuple2;
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {

                return integer + integer2;
            }
        });

        stringIntegerJavaPairDStream.print();


        /**
         * flatmap 将一行拆分为多行
         * 返回迭代器
         */

        javaStreamingContext.sparkContext().setLogLevel("warn");
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
