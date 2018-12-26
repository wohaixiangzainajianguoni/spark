package com.zqg.spark.transforation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;

public class KeyValue {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local") ;
        sparkConf.setAppName("键值对");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        ArrayList<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
  tuple2s.add(new Tuple2<>(1,2));
  tuple2s.add(new Tuple2<>(3,4));
  tuple2s.add(new Tuple2<>( 3,6));

        JavaPairRDD<Integer, Integer> data = context.parallelizePairs(tuple2s);

//        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = data.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
        JavaPairRDD<Integer, Iterable<Integer>> integerIterableJavaPairRDD = data.groupByKey();


//        integerIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
//        @Override
//        public void call(Tuple2<Integer, Integer> stringStringTuple2) throws Exception {
//            System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
//        }
//    });

       integerIterableJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, Iterable<Integer>>>() {
           @Override
           public void call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {

               System.out.println(integerIterableTuple2._1+"---"+integerIterableTuple2._2);
           }
       });



//        integerIterableJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
//            @Override
//            public void call(Tuple2<Integer, Integer> stringStringTuple2) throws Exception {
//                System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
//            }
//        });

    }
}
