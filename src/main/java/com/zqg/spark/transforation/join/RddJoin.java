package com.zqg.spark.transforation.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 合并键一致的值数据
 */
public class RddJoin {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("RddJoin");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(
                new Tuple2<>(1, "黄老邪"),
                new Tuple2<>(2, "郭靖"),
                new Tuple2<>(3, "周伯通")
        );
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = context.parallelizePairs(tuple2s);

        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<>(1, 98),
                new Tuple2<>(2, 45),
                new Tuple2<>(3, 45));
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD =
                context.parallelizePairs(scores);

        integerStringJavaPairRDD.join(integerIntegerJavaPairRDD).foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple2) throws Exception {

                System.out.println(tuple2._1+":"+tuple2._2);
            }
        });


    }
}
