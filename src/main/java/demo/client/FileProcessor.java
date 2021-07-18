package demo.client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FileProcessor {

    public static void main(String[] args){
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","d:/Installed/Hadoop/");

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        sparkConf.set("spark.testing.memory", "2147480000");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        initialRdd.collect().forEach(System.out::println);
        sc.close();
    }
}
