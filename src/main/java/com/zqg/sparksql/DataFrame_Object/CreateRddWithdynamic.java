package com.zqg.sparksql.DataFrame_Object;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class CreateRddWithdynamic {

    public static void main(String[] args) {
        JavaSparkContext javaSparkContext =

                ContextUtil.getJavaSparkContext();

        SQLContext sqlContext=new SQLContext(javaSparkContext);

        JavaRDD<String> file = javaSparkContext.textFile("D:\\springbootworkspace\\spark\\src\\person.txt");

        JavaRDD<Row> map = file.map(new Function<String, Row>() {

            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        s.split(",")[0],
                        s.split(",")[1],
                        s.split(",")[2]
                );
            }
        });

        List<StructField> structFields = Arrays.asList(
                /**
                 * 第一个参数是键值
                 * 第二个参数是键的参数类型
                 *  第三个参数是当前参数是否为空
                 */
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );

        StructType structType =
                DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(map,structType);

        dataFrame.show();
        javaSparkContext.close();

    }
}
