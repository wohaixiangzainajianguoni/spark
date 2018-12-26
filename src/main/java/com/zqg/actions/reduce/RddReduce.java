package com.zqg.actions.reduce;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.naming.Context;
import java.util.Arrays;
import java.util.List;

public class RddReduce{
    public static void main(String[] args) {
        JavaSparkContext context = ContextUtil.getJavaSparkContext();

        List<Integer> list = Arrays.asList(1, 2, 3, 4);

        JavaRDD<Integer> parallelize = context.parallelize(list);

        List<Integer> integers = parallelize.collect();
        System.out.println(integers);
        //        System.out.println(parallelize.count());

    }

}
