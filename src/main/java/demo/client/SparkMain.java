package demo.client;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

//Spark Entry point using JavaSparkContext
public class SparkMain {

    public static void main(String[] args){

        List<Double> inputData = new ArrayList<>();
        inputData.add(23.80);
        inputData.add(14.90);
        inputData.add(43.55);
        inputData.add(29.80);
        inputData.add(56.80);
        inputData.add(11.87);
        inputData.add(19.01);

        //Use standalone spark local instance
        SparkConf sparkConf = new SparkConf(false).setAppName("startingSpark").setMaster("local[*]");
        //Below config setting required to avoid error: System memory 259522560 must be at least 471859200.
        // Please increase heap size using the --driver-memory option or spark.driver.memory in Spark configuration.
        sparkConf.set("spark.testing.memory", "2147480000");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<Double> inputRdd = context.parallelize(inputData);

        //see collect to avoid serialization exception on a multicore system
        //java.io.NotSerializableException: java.io.PrintStream
        //inputRdd.collect().forEach(System.out::println);

        long count = inputData.size();
        System.out.format("Data size =%d\n", count);
        context.close();
    }
}
