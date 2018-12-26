package com.zqg.spark.transforation.union;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Union {
    public static void main(String[] args) {


        JavaSparkContext context = ContextUtil.getJavaSparkContext();

        List<Integer> integers = Arrays.asList(1, 2, 3, 4);
        List<Integer> integers1 = Arrays.asList(4, 5, 6);

        JavaRDD<Integer> parallelize = context.parallelize(integers);

        JavaRDD<Integer> parallelize1 = context.parallelize(integers1);
        parallelize.union(parallelize1).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


    }
}
