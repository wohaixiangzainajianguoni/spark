package sparksql;

import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.janino.Java;
import sun.plugin2.message.JavaScriptBaseMessage;

public class RddtoDataFrame {


    /**
     *  josn to  dataframe
     * @param args
     */
    public static void main(String[] args) {
//        dataToDataFrame();
//        rddToDataFrame();



    }


    public  static  void rddToDataFrameSecond(){
        SparkConf sparkConf = new SparkConf().setAppName("RddToDataFrame").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(context);



        JavaRDD<String> file = context.textFile("D:\\BaiduNetdiskDownload\\光环剩余视频\\spark\\17第十七天_Dataframe\\资料\\resources\\people.txt");



    }

    /**
     * rdd 转换成dataFrame  的方式
     */
    public static   void  rddToDataFrame(){

        SparkConf sparkConf = new SparkConf().setAppName("RddToDataFrame").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(context);



        JavaRDD<String> file = context.textFile("D:\\BaiduNetdiskDownload\\光环剩余视频\\spark\\17第十七天_Dataframe\\资料\\resources\\people.txt");


        JavaRDD<Person> personJavaRDD = file.map(line -> {


            String[] split = line.split(", ");

            String  name = split[0];
            String age = split[1];
            int ageInt = Integer.parseInt(age);
            return  new Person(name,ageInt);

        });


        Dataset<Row> dataFrame = sqlContext.createDataFrame(personJavaRDD, Person.class);

//        dataFrame.show();

        dataFrame.registerTempTable("person");
        Dataset<Row> sql = sqlContext.sql("select name,age from  person");
        sql.show();
    }

    public  static   void dataToDataFrame (){
        SparkConf sparkConf = new SparkConf().setAppName("sql").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> json = sqlContext.read().json("D:\\BaiduNetdiskDownload\\光环剩余视频\\spark\\17第十七天_Dataframe\\资料\\resources\\people.json");
        json.registerTempTable("person");
        Dataset<Row> sql = sqlContext.sql("select * from person where  age is null ");
        sql.show();
    }

}
