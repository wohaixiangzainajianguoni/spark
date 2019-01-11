package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.List;

public class log {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("log");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        context.setLogLevel("warn");
        JavaRDD<String> file = context.textFile("D:\\BaiduNetdiskDownload\\光环剩余视频\\spark\\17第十七天_Dataframe\\资料\\log.txt");

        JavaRDD<LogModel> map = file.map(line -> {

            LogModel logModel = LogUtil.parseLog(line);
            return logModel;

        });



        SQLContext sqlContext = new SQLContext(context);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, LogModel.class);

        dataFrame.registerTempTable("log");
        Dataset<Row> sql = sqlContext.sql("select min(contentSize) from  log");


//        dataFrame.show();
//        dataFrame.registerTempTable("log");
//        Dataset<Row> sql = sqlContext.sql("select min() from log");
//        sql.show();





    }
}
