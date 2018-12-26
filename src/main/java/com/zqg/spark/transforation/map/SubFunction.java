package com.zqg.spark.transforation.map;

import org.apache.spark.api.java.function.Function;


public class SubFunction implements Function<Integer, Integer> {


    @Override
    public Integer call(Integer integer) throws Exception {
        return  integer*integer;
    }
}
