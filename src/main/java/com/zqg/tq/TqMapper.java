package com.zqg.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.jboss.netty.util.internal.StringUtil;
import scala.Int;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TqMapper  extends Mapper<Object, Text,TQ, Text>{

    TQ tq=new TQ();
   Text text=new Text();


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = StringUtils.split(value.toString(), '\t');


        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy");
        try {
            Date parse = simpleDateFormat.parse(split[0]);
            parse.getYear();
            Calendar  calendar=Calendar.getInstance();
            calendar.setTime(parse);

            tq.setYear(calendar.get(Calendar.YEAR));
            tq.setMonth(calendar.get(Calendar.MONTH)+1);
            tq.setDay(calendar.get(Calendar.DAY_OF_MONTH));

            int wd=
                    Integer.parseInt(     split[1].substring(0,split.length-1));
               tq.setWd(wd);

               context.write(tq,new Text(wd+""));

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
