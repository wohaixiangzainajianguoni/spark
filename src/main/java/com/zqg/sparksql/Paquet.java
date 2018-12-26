package com.zqg.sparksql;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.awt.event.ContainerEvent;

public class Paquet {
    public static void main(String[] args) {

        JavaSparkContext context = ContextUtil.getJavaSparkContext();

        SQLContext sqlContext=new SQLContext(context);
        JavaRDD<String> file = context.textFile("D:\\springbootworkspace\\spark\\src\\json");

        Dataset<Row> json = sqlContext.read().json(file);

        json.show();
        /**
         * 存储文件数据
         */
//        json.write().mode(SaveMode.Overwrite).format("parquet").save("D:\\springbootworkspace\\spark\\result");

        /**
         * 存储文件的方式
         * 存储结果文件的文件夹不能存在，不然会报错
         */
        json.write().mode(SaveMode.Ignore).parquet("D:\\springbootworkspace\\spark\\result");


        /**
         * 读取文件格式数据
         *
         */
//        Dataset<Row> parquet = sqlContext.read().format("parquet").load("D:\\springbootworkspace\\spark\\result");

        Dataset<Row> parquet = sqlContext.read().parquet("D:\\springbootworkspace\\spark\\result");
        parquet.show();
//        parquet.show();


    }
}
