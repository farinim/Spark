package demo.client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordRankCalculator {
    public static void main(String[] args){
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir","d:/Installed/Hadoop/");

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        sparkConf.set("spark.testing.memory", "2147480000");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        JavaRDD<String> alphaChars = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()).filter(sentence -> sentence.trim().length() >0);
        JavaRDD<String> words = alphaChars.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator()).filter( word -> word.length() > 0);
        JavaPairRDD<String, Long> wordCounter = words.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> wordTotalCount = wordCounter.reduceByKey(( value1, value2 ) -> value1 + value2);
        JavaPairRDD<Long, String> countToWord = wordTotalCount.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        List<Tuple2<Long, String>> top3Words = countToWord.sortByKey(false).take(3);

        top3Words.forEach(System.out::println);
        sc.close();
    }
}
