package com.zqg.sparksql;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Iterator;

public class Sparksql {


    /**
     * 读取json 数据封装成dataFrame
     * 注意事项：json 中只能还有一层结构，不能含有两层嵌套， 否则无法解析第二层数据
     * @param args
     */
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = ContextUtil.getJavaSparkContext();

        SQLContext sqlContext=new SQLContext(javaSparkContext);
        Dataset<Row> json = sqlContext.read().format("json").load("D:\\springbootworkspace\\spark\\src\\json");


        json.registerTempTable("t1");
        Dataset<Row> sql = sqlContext.sql("select  * from t1 ");
        /**
         * 打印约束
         *
         *  |-- age: string (nullable = true)
         *  |-- name: string (nullable = true)
         */
        sql.printSchema();


        JavaRDD<Row>  javaRDD= sql.javaRDD();
           javaRDD.foreach(new VoidFunction<Row>() {
               @Override
               public void call(Row row) throws Exception {

                   System.out.println(row);

                   System.out.println(row.get(0));
                   System.out.println(row.get(1));
               }
           });

        System.out.println("***************************");

        javaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                while (rowIterator.hasNext())
                {
                    System.out.println(rowIterator.next());
                }
            }
        });


        javaSparkContext.stop();
    }
}
