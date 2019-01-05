package com.wyx.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class InitSpark {
    public static void main(String[] args) {

        //初始化spark
        SparkConf conf=new SparkConf().setMaster("local").setAppName("Wyx App");
        JavaSparkContext sc=new JavaSparkContext(conf);





    }

}
