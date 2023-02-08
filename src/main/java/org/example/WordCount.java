package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void wordCount(String filename){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> inputFile = sparkContext.textFile(filename);
//        JavaRDD<String> wordsFormFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
//        JavaPairRDD countData = wordsFormFile.mapToPair(t -> new Tuple2(t,1)).reduceByKey((x,y) -> (int) x + (int) y);
//        countData.saveAsTextFile("C:\\Users\\Cool\\Downloads\\OutputPath");
//        System.out.println(countData.collect());

//        SparkConf conf = new SparkConf().setAppName(WordCount.class.getName());
        JavaSparkContext sc = null;
            sc = new JavaSparkContext(sparkConf);


            SparkSession spark = SparkSession
                    .builder()
                    .appName("Word Counter")
                    .master("local[1]")
                    .getOrCreate();
        JavaRDD<String> rdd = sc.textFile("C:\\Users\\Cool\\Downloads\\test.csv");
        Dataset<Row> df = spark.createDataFrame(rdd, String.class);
        df.createOrReplaceTempView("temp_table");

        Dataset<Row> result = spark.sql("SELECT * FROM temp_table");
        result.show();
        sc.close();

        /*SparkConf conf= new SparkConf().setAppName("Java Spark").setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        String path = "C:\\Users\\Cool\\Downloads\\test.csv";

        Dataset<Row> df = spark.read().csv(path);
        df.show();
// +------------------+
// |               _c0|
// +------------------+
// |      name;age;job|
// |Jorge;30;Developer|
// |  Bob;32;Developer|
// +------------------+

// Read a csv with delimiter, the default delimiter is ","
        Dataset<Row> df2 = spark.read().option("delimiter", ";").csv(path);
        df2.show();
// +-----+---+---------+
// |  _c0|_c1|      _c2|
// +-----+---+---------+
// | name|age|      job|
// |Jorge| 30|Developer|
// |  Bob| 32|Developer|
// +-----+---+---------+

// Read a csv with delimiter and a header
        Dataset<Row> df3 = spark.read().option("delimiter", ";").option("header", "true").csv(path);
        df3.show();
// +-----+---+---------+
// | name|age|      job|
// +-----+---+---------+
// |Jorge| 30|Developer|
// |  Bob| 32|Developer|
// +-----+---+---------+

// You can also use options() to use multiple options
        java.util.Map<String, String> optionsMap = new java.util.HashMap<String, String>();
        optionsMap.put("delimiter",";");
        optionsMap.put("header","true");
        Dataset<Row> df4 = spark.read().options(optionsMap).csv(path);

// "output" is a folder which contains multiple csv files and a _SUCCESS file.
        df3.write().csv("output");

// Read all files in a folder, please make sure only CSV files should present in the folder.
        String folderPath = "examples/src/main/resources";
        Dataset<Row> df5 = spark.read().csv(folderPath);
        df5.show();
// Wrong schema because non-CSV files are read
// +-----------+
// |        _c0|
// +-----------+
// |238val_238|
// |  86val_86|
// |311val_311|
// |  27val_27|
// |165val_165|
// +-----------+*/
    }
}

