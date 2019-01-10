package second;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Arrays;

public class BroadCast {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("braodcast");
        sparkConf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        Broadcast<Integer> broadcast = context.broadcast(123);






        System.out.println(  broadcast.value());

        Accumulator<Integer> accumulator = context.accumulator(0);
        System.out.println(
        accumulator.value());

        JavaRDD<Integer> parallelize = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));

         parallelize.foreach(new VoidFunction<Integer>() {
             @Override
             public void call(Integer integer) throws Exception {

                 accumulator.add(integer);

             }
         });
        System.out.println(accumulator.value());
        parallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {

                System.out.println(integer*broadcast.value());
            }
        });
    }
}
