import org.apache.spark.sql.SparkSession;

public class SparkHandlerForTests {
    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .master("local[*]")
                .config("spark.testing.memory", "2147480000")
                .getOrCreate();
    }
}
