package com.zqg.spark.transforation.cogroup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CoGroup {


    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("wordcount");
        sparkConf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);


        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "岳不群"),
                new Tuple2<Integer, String>(3, "令狐冲")
        );
        JavaPairRDD<Integer, String> data1 = context.parallelizePairs(tuple2s);

        List<Tuple2<Integer, Integer>> score = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 15),
                new Tuple2<Integer, Integer>(3, 125),
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 15),
                new Tuple2<Integer, Integer>(3, 125)
        );

        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = context.parallelizePairs(score);
        data1.cogroup(integerIntegerJavaPairRDD).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2) throws Exception {

                System.out.println(integerTuple2Tuple2._1+":"+integerTuple2Tuple2._2);
            }
        });

    }
}
