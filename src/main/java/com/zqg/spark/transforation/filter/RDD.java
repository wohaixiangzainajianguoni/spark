package com.zqg.spark.transforation.filter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.List;

public class RDD {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf();
        conf.setAppName("RDD");
        conf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
//        context.textFile("");

        List<String> list=new ArrayList<>();
        list.add("zqg");
        list.add("jwh");
        list.add("hzy");
        list.add("lxy");

        JavaRDD<String> parallelize = context.parallelize(list);
        parallelize.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {

                System.out.println(s);
            }
        });


//        JavaRDD<String> jwh = parallelize.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//
//
//                return s.contains("jwh");
//
//            }
//        });

        JavaRDD<String> jwh = parallelize.filter(new contain("jwh"));
        jwh.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println( s);
            }
        });

        JavaRDD<Object> jwh1 = parallelize.map(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                return s.contains("jwh");
            }
        });

        jwh1.foreach(new VoidFunction<Object>() {


            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });




//        jwh.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

    }



}
