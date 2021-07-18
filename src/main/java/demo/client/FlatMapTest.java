package demo.client;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapTest {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","d:/Installed/Hadoop/");
        List<String> inputData = new ArrayList<>();
        inputData.add("World is not enough");
        inputData.add("Lets do this");

        SparkConf sparkConf = new SparkConf(false).setAppName("startingSpark").setMaster("local[*]");
        sparkConf.set("spark.testing.memory", "2147480000");
        JavaSparkContext context = new JavaSparkContext(sparkConf);


        JavaRDD<String> sentences = context.parallelize(inputData);
        JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        JavaRDD<String> filtered = words.filter(value -> value.length() > 1);
        //System.out.println(filtered.count());
        filtered.collect().forEach(System.out::println);
        context.close();
    }
}
