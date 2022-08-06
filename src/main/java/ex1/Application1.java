package ex1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application1 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("tp1 spark").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);//T1

        JavaRDD<Double> rdd1=sc.parallelize(Arrays.asList(130.0,0.5,20.0,19.0,50.0,8.5,10.0,11.7));
        JavaRDD<Double> rdd2=rdd1.flatMap(  x -> Arrays.asList(x.doubleValue()).iterator());
        JavaRDD<Double> rdd3=rdd2.filter(a -> a>10);
        JavaRDD<Double> rdd4=rdd2.filter(a -> a<10);
        JavaRDD<Double> rdd5=rdd2.filter(a -> a>15);
        JavaRDD<Double> rdd6=rdd3.union(rdd4);
        JavaPairRDD<Double,Integer> rdd71=  rdd5.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairRDD<Double,Integer> rdd7=rdd71.reduceByKey((a, b) -> a+b);
        JavaPairRDD<Double,Integer> rdd81=  rdd6.mapToPair(s -> new Tuple2<>(s,5));
        JavaPairRDD<Double,Integer> rdd8=rdd81.reduceByKey((a, b) -> a-b);
        JavaPairRDD<Double, Integer> rdd9=rdd7.union(rdd8);
        JavaPairRDD<Double,Integer> rdd10=rdd9.sortByKey();

        List<Tuple2<Double, Integer>> elems=rdd10.collect();
        for (Tuple2<Double, Integer> t:elems) {
            System.out.println(t.toString());
        }

    }
}
