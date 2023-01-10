import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.split;

public class SparkSample {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-3.0.0");
        SparkSession spark = SparkSession.builder()
                .appName("SparkSample")
                .master("local[1]")
                .getOrCreate();
        //Read file
        Dataset<Row> ds = spark.read().text("C:\\Users\\piranavan\\IdeaProjects\\Spark_Test\\src\\main\\resources\\cm_pr_live.csv").toDF("value");
        ds.show(false);
        ds.printSchema();
        System.out.println("App Name : "+ spark.sparkContext().appName());
        System.out.println("Deploy Mode : "+ spark.sparkContext().deployMode());
//        Dataset<Row> ds1 = ds.select(split(ds.col("value"), ",")).toDF("new_value");
//        ds1.show(false);
//        ds1.printSchema();
    }
}