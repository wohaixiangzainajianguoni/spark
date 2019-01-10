package second.logannalsis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;

import java.util.Comparator;
import java.util.List;

public class log {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();


        sparkConf.setMaster("local");
        sparkConf.setAppName("log");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        context.setLogLevel("warn");

        JavaRDD<String> file = context.textFile("D:\\BaiduNetdiskDownload\\光环剩余视频\\spark\\17第十七天_Dataframe\\资料\\log.txt");


        JavaRDD<Integer> map = file.map(s -> {
            return Integer.parseInt(s.split("#")[6]);
        });



        Integer reduce = file.map(s -> {
            return Integer.parseInt(s.split("#")[6]);
        }).reduce((o1, o2) -> {
            return o1 + o2;
        });

        long count = file.count();
        double pingjunzhi=reduce/count;
        System.out.println("平均值:"+pingjunzhi);



        JavaRDD<Integer> integerJavaRDD = map.sortBy(new Function<Integer, Integer>() {
                                                         @Override
                                                         public Integer call(Integer integer) throws Exception {
                                                             return  integer;
                                                         }

            /**
             *
             *第二个参数是否是升序排序
             *true从小到达
             *false  从大到小
             */
                                                     }, false, 1
        );

        Integer first = integerJavaRDD.first();
        System.out.println("最大值:"+first);

        List<Integer> integers =
                integerJavaRDD.takeOrdered(1);

        System.out.println("最小值"+integers.get(0));

    }
}
