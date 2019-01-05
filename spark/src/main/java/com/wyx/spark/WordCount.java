package com.wyx.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 2-10:Java 版本的  数 计应用  时 需要
 *
 * @author WangYuxiao
 * @date 19/1/3
 */
public class WordCount {
    public static void main(String[] args) {

        //初始化spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);


        String inputFile = "/Users/apple/opt/testFolder/字典/empty.txt";

        //输入数据
        JavaRDD<String> input = sc.textFile(inputFile);

        //切分为单词
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) {
                        List<String> strings = Arrays.asList(s.split(" "));
                        return strings.iterator();
                    }
                }

        );

        //转换为键值对并计数
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s, 1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
        String outputFile = "/Users/apple/opt/testFolder/test.txt";
        counts.saveAsTextFile(outputFile);


    }

}
