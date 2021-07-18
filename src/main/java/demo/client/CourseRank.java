package demo.client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CourseRank {
    public static void main( String[] args ) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "d:/Installed/Hadoop/");

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        sparkConf.set("spark.testing.memory", "2147480000");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<Integer, String> courseTitles = loadCourseTitles(sc);
        JavaPairRDD<Integer, Integer> courseChapters = loadCourseChapters(sc);
        JavaPairRDD<String, Integer> visits = loadChapterVisited(sc);

        /*
        //Chapters per course:
        JavaPairRDD<Integer, Long> chaptersPerCourse = courseChapters.mapToPair(tuple -> new Tuple2<>(tuple._1, 1L));
        JavaPairRDD<Integer, Long> courseRanks = chaptersPerCourse.reduceByKey(( value1, value2 ) -> value1 + value2);

        //courseRanks.collect().forEach(System.out::println);

        JavaPairRDD<String, Long> titleRanks = courseTitles.leftOuterJoin(courseRanks).mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2.orElse(0L)));
        JavaPairRDD<Long, String> switched = titleRanks.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        switched.sortByKey(false).map(tuple -> tuple._2 + ": "+ tuple._1).collect().forEach(System.out::println);*/

        //Distinc visits
        JavaPairRDD<Integer, Long> chapterDistinctVisits = visits.distinct().mapToPair(tuple -> new Tuple2<>(tuple._2, 1L)).reduceByKey(( value1, value2 ) -> value1 + value2);
        JavaPairRDD<Integer, Tuple2<Integer, Optional<Long>>> chapterCourseVisits = courseChapters.mapToPair(tuple -> new Tuple2(tuple._2,tuple._1)).leftOuterJoin(chapterDistinctVisits);
        //chapterCourseVisits.collect().forEach(System.out::println);
        JavaPairRDD<Integer, Long> courseVisits = chapterCourseVisits.mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2.orElse(0L))).reduceByKey(( value1, value2 ) -> value1 + value2);
        JavaPairRDD<String, Long> coursePopularity = courseTitles.join(courseVisits).mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2));
        coursePopularity.map(tuple -> tuple._1 + " : " + tuple._2).collect().forEach(System.out::println);

        //TODO - compute % of course  content visited by a users, how many of the total chapters of a course were attentended by users.
    }

    private static JavaPairRDD<String, Integer> loadChapterVisited( JavaSparkContext sc ) {
        List<Tuple2<String,Integer>> visitedChapters = new ArrayList<>();
        visitedChapters.add(new Tuple2<>("Alicia",14));
        visitedChapters.add(new Tuple2<>("Alicia",10));
        visitedChapters.add(new Tuple2<>("Alicia",1));
        return sc.parallelizePairs(visitedChapters);
    }

    private static JavaPairRDD<Integer, Integer> loadCourseChapters( JavaSparkContext sc ) {
        List<Tuple2<Integer,Integer>> courseChapters = new ArrayList<>();
        courseChapters.add(new Tuple2<>(1,1));
        courseChapters.add(new Tuple2<>(1,10));
        courseChapters.add(new Tuple2<>(2,14));
        courseChapters.add(new Tuple2<>(3,21));
        return sc.parallelizePairs(courseChapters);
    }

    private static JavaPairRDD<Integer, String> loadCourseTitles( JavaSparkContext sc ) {
        List<Tuple2<Integer,String>> courseTitles = new ArrayList<>();
        courseTitles.add(new Tuple2(1, "Apache Spark"));
        courseTitles.add(new Tuple2(2, "Hadoop"));
        courseTitles.add(new Tuple2(3, "SparkSQL"));
        return sc.parallelizePairs(courseTitles);
    }
}
