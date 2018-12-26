package com.zqg.spark.transforation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;

public class RddCllectionOprations {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("集合操作");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        ArrayList<String> strings = new ArrayList<>();

        strings.add("coffee");
        strings.add("coffee");
        strings.add("tea");
        strings.add("monkey");
        strings.add("money");
        JavaRDD<String> stringJavaRDD = context.parallelize(strings);

        JavaRDD<String> distinct = stringJavaRDD.distinct();
//        sout(distinct);
        ArrayList<String> objects = new ArrayList<>();

        objects.add("coffee");
        objects.add("tea");
        objects.add("kitty");
        JavaRDD<String> javaRDD = context.parallelize(objects);

//        求交集
        JavaRDD<String> javaRDD1 = stringJavaRDD.intersection(javaRDD);
//        sout(javaRDD1);
//    求差集在第一个rdd 中存在但是在第二个rdd 中不存在元素
        JavaRDD<String> subtract = stringJavaRDD.subtract(javaRDD);
//        sout(subtract);

        JavaRDD<String> union = stringJavaRDD.union(javaRDD);

//        sout(union);

        JavaRDD<String> unionDistinct = union.distinct();
        sout(unionDistinct);

        unionDistinct.persist(StorageLevel.MEMORY_AND_DISK());

    }

    static void  sout(JavaRDD  javaRDD){
        javaRDD.foreach(new VoidFunction() {
            @Override
            public void call(Object line) throws Exception {
                System.out.println(line);
            }
        });
    }



}
