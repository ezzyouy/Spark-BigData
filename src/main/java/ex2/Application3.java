package ex2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("RDD3").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddLines=sc.textFile("hdfs://localhost:9000/ville.txt");
        JavaRDD<String> rddRows=rddLines.flatMap(s ->
                Arrays.asList(s.split("\n")).iterator());


        String an="2022";
        JavaRDD<String> rdd_dates=rddRows.filter(s ->s.contains(an));
        List<String> elems = rddRows.collect();
        for (String t : elems) {
            System.out.println(t.toString());
        }
        JavaPairRDD<String, Double> rddPairs = rdd_dates
                .mapToPair(s -> new Tuple2<>((s.split(" "))[1],
                        Double.valueOf(s.split(" ")[3]) ));
        JavaPairRDD<String,Double> ventC=rddPairs.reduceByKey((a, b) -> a+b);

        List<Tuple2<String, Double>>   el = ventC.collect();
        for (Tuple2<String, Double> t : el) {
            System.out.println(t.toString());
        }

        ventC.saveAsTextFile("hdfs://localhost:9000/ventA.txt");


    }


}
