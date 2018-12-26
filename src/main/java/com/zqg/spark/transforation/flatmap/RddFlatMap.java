package com.zqg.spark.transforation.flatmap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class RddFlatMap {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("flatMap");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        ArrayList<String> strings = new ArrayList<>();

        strings.add("you jump");
        strings.add("i jump");

        JavaRDD<String> parallelize = context.parallelize(strings);

        parallelize.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        /**
         * 将一行数据拆分为多行数据
         */
        JavaRDD<String> flatMap = parallelize.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        flatMap.first();

        JavaPairRDD<String, Integer> pairRdd = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reducePairRDD = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });




        reducePairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
            }
        });
        JavaPairRDD<Integer,String> objectObjectJavaPairRDD = reducePairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer,String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2,stringIntegerTuple2._1);
            }
        });

//         objectObjectJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
//             @Override
//             public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//
//                 System.out.println(integerStringTuple2._1+":"+integerStringTuple2._2);
//             }
//         });

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = objectObjectJavaPairRDD.sortByKey(false);
        integerStringJavaPairRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {

                System.out.println(integerStringTuple2._2+":"+integerStringTuple2._1);
            }
        });


        System.out.println("--------------------");
        pairRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
            }
        });


    }


}
