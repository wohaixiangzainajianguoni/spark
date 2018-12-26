package com.zqg.spark.transforation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ContextUtil {

    public  static JavaSparkContext getJavaSparkContext(){

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("rddunion");
        sparkConf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        return context;
    }
}
