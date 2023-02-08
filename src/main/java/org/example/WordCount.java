package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void wordCount(String filename){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile(filename);
        JavaRDD<String> wordsFormFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD countData = wordsFormFile.mapToPair(t -> new Tuple2(t,1)).reduceByKey((x,y) -> (int) x + (int) y);
        countData.saveAsTextFile("OutputPath");
    }
}
