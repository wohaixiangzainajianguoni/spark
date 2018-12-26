package com.zqg.sort;

import com.zqg.spark.transforation.ContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

/**
 * 二次排序
 */
public class SecondSort {
    public static void main(String[] args) {

        JavaSparkContext  context = ContextUtil.getJavaSparkContext();

        JavaRDD<String> stringJavaRDD = context.textFile("C:\\Users\\junmei02\\Desktop\\data.txt");

        /**
         * 三个泛型，一个泛型便是输入的类型，
         * 第二个泛型输出键值对的键的类型，
         * 第三个泛型是上输出的类型键值对的值的泛型
         */
        JavaPairRDD<Bean, String> beanStringJavaPairRDD = stringJavaRDD.mapToPair(new PairFunction<String, Bean, String>() {

            /**
             * 参数表示的每一行的数据
             * @param s
             * @return
             * @throws Exception
             */
            @Override
            public Tuple2<Bean, String> call(String s) throws Exception {

                String[] split = s.split(",");
                Bean bean = new Bean(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
                return new Tuple2<>(bean,s);

            }
        });


        JavaPairRDD<Bean, String> rdd = beanStringJavaPairRDD.sortByKey();
        rdd.sortByKey().foreach(new VoidFunction<Tuple2<Bean, String>>() {
            @Override
            public void call(Tuple2<Bean, String> tuple2) throws Exception {

                System.out.println(tuple2._2);

                System.out.println(tuple2._1.toString());


            }
        });

    }
}
