package second.logannalsis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

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
        JavaRDD<LogModel> logData = file.map(s -> {
            return LogUtil.parseLog(s);
        });
        logData.cache();
        JavaRDD<Long> content = logData.map(log -> {
            return log.getContentSize();
        });
        long count = content.count();
        System.out.println( "个数:"+count);
        Long reduce = content.reduce((v1, v2) -> {
            return v1 + v2;
        });
        System.out.println("总和:"+reduce);
        System.out.println("平均值:"+reduce/count*1.0);
//
//        JavaRDD<Long> contentSortedRdd = content.sortBy(new Function<Long, Object>() {
//            @Override
//            public Object call(Long aLong) throws Exception {
//                return aLong;
//            }
//            /**
//             * 参数为true 表示排序顺序从小到大，
//             * 参数为false 表示拍戏从大到校
//             */
//        },true,1);


        JavaRDD<Long> contentSortedRdd = content.sortBy(s -> {
            return s;
        }, false, 1);

        Long  max = contentSortedRdd.first();

        System.out.println("最小值为:"+max);

        List<Long> min = contentSortedRdd.takeOrdered(1);
        System.out.println("最大值为:"+min.get(0));
        System.out.println(contentSortedRdd.collect());;



        /*
         返回的状态码 的计数
         */
    logData.map(line -> {
            return line.getResponseCode();
        }).mapToPair(s -> {
            return new Tuple2<>(s, 1);
        }).reduceByKey((v1,v2)->{
            return  v1+v2; }).sortByKey(true).foreach(tuple2 -> {
        System.out.println(tuple2._1+":"+tuple2._2);

    });

        /**
         * 访问次数超过N 的ip地址
         */

        JavaPairRDD<String, Integer> sortByKey = logData.map(logModel -> {
            return logModel.getIpAddress();
        }).mapToPair(ip -> {
            return new Tuple2<>(ip, 1);
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        }).sortByKey(true);

        System.out.println(sortByKey.collect());

        int  n=2;
        JavaPairRDD<String, Integer> filter = sortByKey.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                if (tuple2._2 >n )
                    return true;
                else
                    return false;

            }
        });

        System.out.println(filter.collect());




        JavaPairRDD<String, Integer> filter1 = sortByKey.filter(tuple2 -> {
            if (tuple2._2 > n) {
                return true;
            } else {
                return false;
            }
        });
        System.out.println(filter1.collect());

        /**
         *访问次数的最多的前三名
         */
        System.out.println("top N");

        List<Tuple2<String, Integer>> take = logData.mapToPair(new PairFunction<LogModel, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(LogModel logModel) throws Exception {

                return new Tuple2<>(logModel.getUrl(), 1);

            }
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        }).sortByKey(true).take(3);

        System.out.println(take);


    }
}
