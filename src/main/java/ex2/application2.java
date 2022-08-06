package ex2;

import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class application2 {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf=new SparkConf().setAppName("RDD2").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddLines=sc.textFile("hdfs://localhost:9000/ville.txt");
        JavaRDD<String> rddNames=rddLines.flatMap(s -> Arrays.asList(s.split("\n")).iterator());

        JavaPairRDD<String, Double> rddPairs = rddNames
                .mapToPair(s -> new Tuple2<>((s.split(" "))[1],
                        Double.valueOf(s.split(" ")[3]) ));

        JavaPairRDD<String,Double> ventecount=rddPairs.reduceByKey((a, b) -> a+b);

        List<Tuple2<String, Double>> elems = ventecount.collect();
        for (Tuple2<String, Double> t : elems) {
            System.out.println(t.toString());
        }

        ventecount.saveAsTextFile("hdfs://localhost:9000/ventCount.txt");

    }
}
