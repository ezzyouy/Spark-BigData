import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Application3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> data=sc.textFileStream("hdfs://localhost:9000/data");
        JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 9999);
    }
}
