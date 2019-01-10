package second.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.sources.In;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RddMap {


    public  static   void printe(String str){
        System.out.println(str);
    }


    public  static   void  map(){

        JavaSparkContext context = getContext();

        List<String> list = Arrays.asList("张无忌", "小赵", "赵敏");
        JavaRDD<String> parallelize = context.parallelize(list);

        /**
         * 泛型一， 输入的数据类型，
         * 泛型二， 输出的数据类型
         *
         *
         *
         */






//        parallelize.map(new Function<String, String>() {
//            @Override
//            public String call(String s) throws Exception {
//                return "hello" + s;
//            }
//        }).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//
//                printe(s);
//            }
//        });
//
//        Function function=new Function() {
//            @Override
//            public Object call(Object o) throws Exception {
//                return null;
//            }
//        };
//        Function lamuda=(s)->{return  "hello"+s};
//
//
//        parallelize.map(function);
        parallelize.map((s) -> {return  "hello"+s;}).foreach((s) -> {printe(s);});


        context.stop();
    }

    public static JavaSparkContext  getContext()
    {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("rddmap");
        sparkConf.setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        return  context;


    }


    public static   void filter(){

        JavaSparkContext context = getContext();

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> parallelize = context.parallelize(integers);
//        parallelize.filter(new Function<Integer, Boolean>() {
//            @Override
//            public Boolean call(Integer integer) throws Exception {
//                return  integer%2==0;
//            }
//        }).foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//
//                System.out.println(integer);
//            }
//        });

        parallelize.filter(s-> {return  s%2==0;}).foreach(s->printe(s+""));

        context.stop();
    }

    public  static  void flatMap()
    {

        JavaSparkContext context = getContext();
        List<String> list = Arrays.asList("you--jump", "i--jump");
        JavaRDD<String> parallelize = context.parallelize(list);
//
//        parallelize.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                System.out.println(s);
//                return Arrays.asList(s.split("--")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s,1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//
//                return  integer+integer2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//
//                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
//            }
//        });


        parallelize.flatMap(s->Arrays.asList(s.split("--")).iterator()).
                mapToPair(s -> new Tuple2<>(s,1) ).
                reduceByKey((o1,o2)->{ return   o1+o2; }).
                foreach(s->printe(s._1+":"+s._2));


        context.stop();

    }

    public  static  void groupBykey()
    {
        JavaSparkContext context = getContext();
        List<Tuple2<String, String>> tuple2s = Arrays.asList(
                new Tuple2<String,String>("武当", "张三丰"),
                new Tuple2<String,String>("峨眉", "灭绝师太"),
                new Tuple2<String,String>("武当", "张无忌"),
                new Tuple2<String,String>("武当", "宋远桥"),
                new Tuple2<String,String>("武当", "宋青书"),
                new Tuple2<String,String>("峨眉", "周芷若"),
                new Tuple2<String,String>("峨眉", "丁敏君")
        );



        JavaRDD<Tuple2<String, String>> parallelize = context.parallelize(tuple2s);

        /**
         *第一个和第二个泛型表示的是数据的类型，
         * 第三个泛型表示的是以那个类型做为分组的键
         */
//        parallelize.groupBy(new Function<Tuple2<String, String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tuple2) throws Exception {
//
//                return  tuple2._1;
//
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2<String, String>>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<Tuple2<String, String>>> tuple2) throws Exception {
//
//                String s = tuple2._1;
//                Iterable<Tuple2<String, String>> tuple2s1 = tuple2._2;
//                Iterator<Tuple2<String, String>> iterator = tuple2s1.iterator();
//               String names="";
//                while ( iterator.hasNext())
//                {
//                    Tuple2<String, String> next = iterator.next();
//                    System.out.println(next);
//                   names+=  next._2;
//                }
//
//                System.out.println(s+"::::::"+names);
//            }
//        });

        parallelize.groupBy(t2->t2._1).foreach(t2->{

            String s = t2._1;
            System.out.println(s);
            Iterator<Tuple2<String, String>> iterator = t2._2.iterator();
            while (iterator.hasNext())
            {
                Tuple2<String, String> next = iterator.next();
                System.out.println(next._2);
            }
        });

        List<Tuple2<Integer, String>> tuple2sInte = Arrays.asList(
                new Tuple2<Integer,String>(1, "张三丰"),
                new Tuple2<Integer,String>(2, "灭绝师太"),
                new Tuple2<Integer,String>(1, "张无忌"),
                new Tuple2<Integer,String>(1, "宋远桥"),
                new Tuple2<Integer,String>(1, "宋青书"),
                new Tuple2<Integer,String>(2, "周芷若"),
                new Tuple2<Integer,String>(2, "丁敏君")
        );

        JavaRDD<Tuple2<Integer, String>> parallelize1 = context.parallelize(tuple2sInte);

//        parallelize1.groupBy(new Function<Tuple2<Integer, String>, Integer>() {
//            @Override
//            public Integer call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//
//                return  integerStringTuple2._1;
//            }
//        }).foreach(new VoidFunction<Tuple2<Integer, Iterable<Tuple2<Integer, String>>>>() {
//            @Override
//            public void call(Tuple2<Integer, Iterable<Tuple2<Integer, String>>> integerIterableTuple2) throws Exception {
//                System.out.println(integerIterableTuple2._1);
//                Iterable<Tuple2<Integer, String>> tuple2s1 = integerIterableTuple2._2;
//                Iterator<Tuple2<Integer, String>> iterator = tuple2s1.iterator();
//                while( iterator.hasNext())
//                {
//                    Tuple2<Integer, String> next = iterator.next();
//                    System.out.println(next._2);
//                }
//
//
//            }
//        });

        parallelize1.groupBy(t2->{
            return  t2._1;
        }).foreach(t2->{
            System.out.println(t2._1);
            Iterable<Tuple2<Integer, String>> tuple2s1 = t2._2;
            Iterator<Tuple2<Integer, String>> iterator = tuple2s1.iterator();
            while (iterator.hasNext())
            {
                Tuple2<Integer, String> next = iterator.next();
                System.out.println(next._2);
            }
        });

        context.stop();
    }

    public  static  void reduceByKey(){
        JavaSparkContext context = getContext();
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(
                new Tuple2<String, Integer>("少林寺", 40),
                new Tuple2<String, Integer>("丐帮", 50),
                new Tuple2<String, Integer>("少林寺", 60),
                new Tuple2<String, Integer>("丐帮", 70)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = context.parallelizePairs(tuple2s);
//        stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                System.out.println(integer);
//                System.out.println(integer2);
//           return      integer+integer2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2._1+":::"+stringIntegerTuple2._2);
//            }
//        });

        stringIntegerJavaPairRDD.reduceByKey((o1,o2)->o1+o2).foreach(t2-> System.out.println(t2._1+":::"+t2._2));


        context.stop();
    }


    public  static  void sortbyKey(){
        JavaSparkContext context = getContext();

        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(
                new Tuple2<Integer, String>(10, "10"),
                new Tuple2<Integer, String>(20, "20"),
                new Tuple2<Integer, String>(30, "30"),
                new Tuple2<Integer, String>(40, "40"),
                new Tuple2<Integer, String>(50, "50")
        );


        JavaPairRDD<Integer, String> integerStringJavaPairRDD = context.parallelizePairs(tuple2s);
        /**
         * ssortByKey
         * 参数为true 表示的是从小到大
         * false  表示的是从大到小
         */
//        integerStringJavaPairRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, String>>() {
//            @Override
//            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//                System.out.println(integerStringTuple2._1+":"+integerStringTuple2._2);
//            }
//        });

        integerStringJavaPairRDD.sortByKey(false).foreach(t2->{
            System.out.println(t2._1+":"+t2._2);
        });
        context.stop();
    }

    public static  void join(){
        JavaSparkContext context = getContext();

        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(
                new Tuple2<Integer, String>(1, "黄老邪"),
                new Tuple2<Integer, String>(2, "周伯通"),
                new Tuple2<Integer, String>(3, "黄蓉"),
                new Tuple2<Integer, String>(4, "郭靖")
        );
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = context.parallelizePairs(tuple2s);
        List<Tuple2<Integer, Integer>> tuple2s1 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(1, 100)
        );

        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = context.parallelizePairs(tuple2s1);
//        integerIntegerJavaPairRDD.join(integerStringJavaPairRDD).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, String>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Integer, String>> tuple2) throws Exception {
//
//                Integer integer = tuple2._1;
//                Tuple2<Integer, String> stringIntegerTuple2 = tuple2._2;
//                System.out.println(integer+":"+stringIntegerTuple2._2+":"+stringIntegerTuple2._1);
//
//            }
//        });
        integerIntegerJavaPairRDD.join(integerStringJavaPairRDD).foreach(t2->{

            Integer integer = t2._1;
            Tuple2<Integer, String> integerStringTuple2 = t2._2;
            System.out.println(integer);
            System.out.println(integerStringTuple2);
        });
        context.stop();
    }

    public  static  void  cogroup() {
        JavaSparkContext context = getContext();

        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "岳不群")
        );


        JavaPairRDD<Integer, String> names = context.parallelizePairs(tuple2s);

        List<Tuple2<Integer, Integer>> tuple2s1 = Arrays.asList(

                new Tuple2<Integer, Integer>(1, 60),
                new Tuple2<Integer, Integer>(1, 60), new Tuple2<Integer, Integer>(1, 60),
                new Tuple2<Integer, Integer>(2, 70),
                new Tuple2<Integer, Integer>(1, 80),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 120),
                new Tuple2<Integer, Integer>(3, 100)
        );
        JavaPairRDD<Integer, Integer> scores = context.parallelizePairs(tuple2s1);

        names.cogroup(scores).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t2) throws Exception {

                Integer integer = t2._1;

                System.out.println(integer);

                Tuple2<Iterable<String>, Iterable<Integer>> iterableIterableTuple2 = t2._2;

                Iterable<String> strings = iterableIterableTuple2._1;
                Iterator<String> iterator = strings.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                Iterator<Integer> iterator1 = iterableIterableTuple2._2.iterator();

                while (iterator1.hasNext()) {
                    System.out.println(iterator1.next());
                }

            }
        });
        context.stop();
    }

    /**
     * 合并元素但是不去去重
     */

    public static  void  union(){

        JavaSparkContext context = getContext();
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> data1 = context.parallelize(integers);


        List<Integer> integers1 = Arrays.asList(6, 2, 3, 4,5,4,5);

        JavaRDD<Integer> data2 = context.parallelize(integers1);


        JavaRDD<Integer> union = data1.union(data2);


       union.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {


                System.out.println(integer);


            }
        });

       context.stop();
    }

    /**
     * 求交集,去重复
     */

    public  static  void  intersection(){
        JavaSparkContext context = getContext();
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5,4,5);
        JavaRDD<Integer> data1 = context.parallelize(integers);
        List<Integer> integers1 = Arrays.asList(6, 2, 3, 4,5,4,5);
        JavaRDD<Integer> data2 = context.parallelize(integers1);
        data1.intersection(data2).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        context.stop();
    }

    /**
     * 去重复
     */
    public  static  void distinct(){
        JavaSparkContext context = getContext();

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 4, 5, 5, 6, 6, 1, 2);

        JavaRDD<Integer> parallelize = context.parallelize(integers);

        parallelize.distinct().foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {

                System.out.println(integer);
            }
        });
        context.stop();
    }


    /**
     * 计算笛卡尔积
     */
    public  static  void cartesian(){

        JavaSparkContext context = getContext();

        List<Integer> integers = Arrays.asList(1, 2, 3);

        JavaRDD<Integer> parallelize = context.parallelize(integers);
        List<Integer> integers1 = Arrays.asList(4, 5, 6);
        JavaRDD<Integer> parallelize1 = context.parallelize(integers1);
        JavaPairRDD<Integer, Integer> cartesian = parallelize.cartesian(parallelize1);

        cartesian.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {

                System.out.println(tuple2);
            }
        });

    }

    /**
     * 功能和map 相似, map 中遍历的是一条数据
     *  mapPatitions  遍历 的是一个分区
     *
     */
    public  static  void mapPatitions(){

        JavaSparkContext context = getContext();
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6,7);
        JavaRDD<Integer> parallelize = context.parallelize(integers,3);
//        JavaRDD<Integer> stringJavaRDD = parallelize.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
//               int  temp=0;
//            @Override
//            public Iterator<Integer> call(Iterator<Integer> iterator) throws Exception {
//                while (iterator.hasNext())
//                {temp++;
//                    System.out.println(iterator.next()+"::::"+temp);
//                }
//                return  iterator;
//            }
//        });
//
        JavaRDD<Integer> objectJavaRDD = parallelize.mapPartitions(iterator ->
        {
            while (iterator.hasNext()) {
                Integer next = iterator.next();
            }

            return  iterator;

        });


        objectJavaRDD.count();
    }



    public static  void repartation(){

        JavaSparkContext context = getContext();
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6,7);
        JavaRDD<Integer> parallelize = context.parallelize(integers,2);

        JavaRDD<Integer> repartition = parallelize.repartition(4);
        repartition.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            int  temp=0;
            @Override
            public Iterator<Integer> call(Iterator<Integer> iterator) throws Exception {
                while (iterator.hasNext())
                {temp++;
                    System.out.println(iterator.next()+"::::"+temp);
                }
                return  iterator;
            }
        }).collect();

    repartition.mapPartitions(iterator ->
            {
                while (iterator.hasNext())
                {
                    Integer next = iterator.next();
                    System.out.println(next);
                }
           return  iterator;
            }
            );


    }
    private static void mapPartitionsWithIndex() {

        JavaSparkContext context = getContext();


        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 8);
        JavaRDD<Integer> parallelize = context.parallelize(integers,3);

        parallelize.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer integer, Iterator<Integer> iterator) throws Exception {

                System.out.println(integer);
                return iterator;
            }
        },true).count();


    }


    public  static  void aggregateByKey(){
        JavaSparkContext context = getContext();

        List<String> list = Arrays.asList("you,jump", "i,jump");

        JavaRDD<String> parallelize = context.parallelize(list);

//        parallelize.flatMap(s->{
//         return   Arrays.asList(
//            s.split(",")
//            ).iterator();
//        }).mapToPair(s->{
//            return  new Tuple2<>(s,1);
//        }).reduceByKey((o1,o2)->{
//          return   o1+o2;
//        }).foreach(t2->{
//
//            System.out.println(t2._1+":"+t2._2);
//        });


        parallelize.flatMap(s->{
            return   Arrays.asList(
                    s.split(",")
            ).iterator();
        }).mapToPair(s->{
            return  new Tuple2<>(s,1);
        }).aggregateByKey(
                0,
                (o1,o2)->{
                    return  o1+o2;
                }
                ,(o1,o2)->{
                    return  o1+o2;
                }
        ).foreach(t2->{
            System.out.println(t2._1+":"+t2._2);
        });



//                aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer+integer2;
//            }
//        }, new Function2<Integer, Integer, Integer>() {
//
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer+integer2;
//            }
//        }).foreach(tuple2 -> {
//
//            System.out.println(tuple2._1+":"+tuple2._2);
//        });



    }


    public static void main(String[] args) {
//       map();
//       filter();
//       flatMap();
//       groupBykey();
//        reduceByKey();
//        sortbyKey();
//        join();
//        cogroup();
//        union();
//        intersection();
//        distinct();
//        cartesian();
//        mapPatitions();//调优的时候经常操作
//        repartation();
//        mapPartitionsWithIndex();
//        aggregateByKey();
//        repartitionAndSortWithinPartitions();


    }

    private static void repartitionAndSortWithinPartitions() {

    }


}
