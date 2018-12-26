package com.zqg.spark.transforation.map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class RddMap {


    public static void main(String[] args) {

        SparkConf  sparkConf=new SparkConf();
        sparkConf.setAppName("RddMap");
        sparkConf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> parallelize = context.parallelize(Arrays.asList(1, 2, 34, 4));

        JavaRDD<Integer> map = parallelize.map(new SubFunction());
        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


    }





}
