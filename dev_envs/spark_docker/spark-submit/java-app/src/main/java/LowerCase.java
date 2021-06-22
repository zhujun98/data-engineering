import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LowerCase {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("spark_test").getOrCreate();

        List<String> logOfSongs = Arrays.asList(
            "Despacito",
            "Nice for what",
            "No tears left to cry",
            "Despacito",
            "Havana",
            "In my feelings",
            "Nice for what",
            "despacito",
            "All the stars"
        );

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> distributedSongLog = sc.parallelize(logOfSongs);
        System.out.println(distributedSongLog.map((x) -> { return x.toLowerCase(); }).collect());

        spark.stop();
    }
}
