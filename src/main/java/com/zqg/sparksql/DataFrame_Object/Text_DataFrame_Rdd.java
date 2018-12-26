package com.zqg.sparksql.DataFrame_Object;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class Text_DataFrame_Rdd {
    public static void main(String[] args) {
        Person  person=new Person();
        JavaSparkContext javaSparkContext = ContextUtil.getJavaSparkContext();


        SQLContext sqlContext=new SQLContext(javaSparkContext);

        JavaRDD<String> file = javaSparkContext.textFile("D:\\springbootworkspace\\spark\\src\\person.txt");


        JavaRDD<Person> map = file.map(new Function<String, Person>() {
            @Override
            public Person call(String s) throws Exception {
                String[] split = s.split(",");
                Person person = new Person(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
                return person;

            }
        });

        map.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception{
                System.out.println(person);;
            }
        });


        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, Person.class);

        dataFrame.registerTempTable("person");
        Dataset<Row> sql = sqlContext.sql("select * from person");
        sql.show();
        javaSparkContext.close();


    }
}
