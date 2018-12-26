package com.zqg.sparksql.dataFrame_mysql;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Concat;
import org.apache.spark.sql.catalyst.parser.SqlBaseBaseListener;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataFrame_Mysql {

    public static void main(String[] args) {

        /**
         * 第一种方式加载数据
         */
        JavaSparkContext context = ContextUtil.getJavaSparkContext();
        SQLContext sqlContext=new SQLContext(context);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("mysql");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf).getOrCreate();





        Properties readConnProperties1 = new Properties();
        readConnProperties1.put("driver", "com.mysql.jdbc.Driver");
        readConnProperties1.put("user", "root");
        readConnProperties1.put("password", "111111");

        readConnProperties1.put("fetchsize", "3");

        Dataset<Row> jdbc = spark.read().jdbc(
                "jdbc:mysql://127.0.0.1:3306/sparksql",
                "person",
                readConnProperties1);

        jdbc.show();

//        Map<String, String> options = new HashMap<>();
//
//        options.put("driver","com.mysql.jdbc.Driver");
//        options.put("url","jdbc.mysql://127.0.0.1:3306/sparksql");
//        options.put("user","root");
//        options.put("password","111111");
//        options.put("dbtable","person");
//
//
//
//
//        Dataset<Row> jdbc = sqlContext.read().format("jdbc").options(options).load();
//
//        jdbc.show();
        /**
         * 第二种方式加载MySQL数据库中的数据
         */

        Dataset<Row> asd = spark.read().jdbc(
                "jdbc:mysql://127.0.0.1:3306/sparksql",
                "person",
                readConnProperties1);
        asd.show();







    }
}
