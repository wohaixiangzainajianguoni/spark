package com.zqg.spark.transforation.filter;

import org.apache.spark.api.java.function.Function;

public class contain    implements Function<String,Boolean > {

    private  String queryStr;

    public contain(String queryStr) {


        this.queryStr = queryStr;
    }

    public contain() {
    }

    @Override
    public Boolean call(String s) throws Exception {
        return s.contains(queryStr);
    }
}
