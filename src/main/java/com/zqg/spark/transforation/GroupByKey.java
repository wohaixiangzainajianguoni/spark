package com.zqg.spark.transforation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GroupByKey {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local");
        sparkConf.setAppName("groupby");
        JavaSparkContext context = new JavaSparkContext(sparkConf);


        List<Tuple2<String, String>> tuple2s = Arrays.asList(
                new Tuple2<String, String>("武当", "张三风"),
                new Tuple2<String, String>("武当", "张无忌"),
                new Tuple2<String, String>("峨眉", "灭绝师太"),
                new Tuple2<String, String>("峨眉", "周芷若")

        );

        JavaRDD<Tuple2<String, String>> parallelize = context.parallelize(tuple2s);

        /**
         * function泛型中的第二个参数表示的是groutbykey 中的key 的类型
         *
         */
        parallelize.groupBy(new Function<Tuple2<String, String>, String >() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {

                //返回的值是用来分组的字段
                return  stringStringTuple2._1;
            }
        }).foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2<String, String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple2<String, String>>> tuple2) throws Exception {

                System.out.print("键:"+tuple2._1+""+"值");
                Iterable<Tuple2<String, String>> tuple2s1 = tuple2._2();

                Iterator<Tuple2<String, String>> iterator =
                        tuple2s1.iterator();

                while (iterator.hasNext())
                {
                    Tuple2<String, String> next = iterator.next();
                    System.out.println(next._2);

                }

            }
        });
    }
}
