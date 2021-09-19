package cs523.BDTFinalProject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
public class KafkaStreamSQL {
    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder()
                .appName("Spark Kafka Integration Structured Tweet Streaming")
                .master("local[*]").getOrCreate();
        Dataset<Row> ds = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "TwitterDataAnalytics").load();
        Dataset<Row> lines = ds.selectExpr("CAST(value AS STRING)");
        Dataset<Row> dataAsSchema = lines.selectExpr("value",
                "split(value,',')[0] as createdAt",
                "split(value,',')[1] as Id", 
                "split(value,',')[2] as UserId",
                "split(value,',')[3] as Location",
                "split(value,',')[4] as FollowersCount",
                "split(value,',')[5] as isVerified",
                "split(value,',')[6] as UserCreatedAt",
                "split(value,',')[7] as TimeZone",
                "split(value,',')[8] as sentiment",
                "split(value,',')[9] as TweetCreatedHour",
                "split(value,',')[10] as TweetCreatedMin",
                "split(value,',')[11] as TweetCreatedSec",
                "split(value,',')[12] as UserCreatedMonth",
                "split(value,',')[13] as UserCreatedYear",
                "split(value,',')[14] as hashtags",
                "split(value,',')[15] as Name", 
                "split(value,',')[16] as Text")
                .drop("value");
        dataAsSchema = dataAsSchema
                .withColumn(
                        "createdAt",
                        functions.regexp_replace(functions.col("createdAt"),
                                " ", ""))
                .withColumn("Id",
                        functions.regexp_replace(functions.col("Id"), " ", ""))
                .withColumn(
                        "UserId",
                        functions.regexp_replace(functions.col("UserId"), " ",
                                ""))
                .withColumn(
                        "Location",
                        functions.regexp_replace(functions.col("Location"),
                                " ", ""))
                .withColumn(
                        "FollowersCount",
                        functions.regexp_replace(
                                functions.col("FollowersCount"), " ", ""))
                .withColumn(
                        "isVerified",
                        functions.regexp_replace(functions.col("isVerified"),
                                " ", ""))
                .withColumn(
                        "UserCreatedAt",
                        functions.regexp_replace(
                                functions.col("UserCreatedAt"), " ", ""))
                .withColumn(
                        "TimeZone",
                        functions.regexp_replace(functions.col("TimeZone"),
                                " ", ""))
                .withColumn(
                        "sentiment",
                        functions.regexp_replace(functions.col("sentiment"),
                                " ", ""))
                 .withColumn(
                        "TweetCreatedHour",
                        functions.regexp_replace(functions.col("TweetCreatedHour"),
                                " ", ""))
                .withColumn(
                        "TweetCreatedMin",
                        functions.regexp_replace(
                                functions.col("TweetCreatedMin"), " ", ""))
                .withColumn(
                        "TweetCreatedSec",
                        functions.regexp_replace(functions.col("TweetCreatedSec"),
                                " ", ""))
                .withColumn(
                        "UserCreatedMonth",
                        functions.regexp_replace(
                                functions.col("UserCreatedMonth"), " ", ""))
                .withColumn(
                        "UserCreatedYear",
                        functions.regexp_replace(functions.col("UserCreatedYear"),
                                " ", ""))
                .withColumn(
                        "hashtags",
                        functions.regexp_replace(functions.col("hashtags"),
                                " ", ""))
                .withColumn(
                        "Name",
                        functions.regexp_replace(functions.col("Name"), " ", ""))
                .withColumn(
                        "Text",
                        functions.regexp_replace(functions.col("Text"), " ", ""));
        dataAsSchema = dataAsSchema
                .withColumn("createdAt",
                        functions.col("createdAt").cast(DataTypes.StringType))
                .withColumn("Id",
                        functions.col("Id").cast(DataTypes.StringType))
                .withColumn("UserId",
                        functions.col("UserId").cast(DataTypes.IntegerType))
                .withColumn("Location",
                        functions.col("Location").cast(DataTypes.StringType))
                .withColumn(
                        "FollowersCount",
                        functions.col("FollowersCount").cast(
                                DataTypes.StringType))
                .withColumn("isVerified",
                        functions.col("isVerified").cast(DataTypes.StringType))
                .withColumn(
                        "UserCreatedAt",
                        functions.col("UserCreatedAt").cast(
                                DataTypes.IntegerType))
                .withColumn("TimeZone",
                        functions.col("TimeZone").cast(DataTypes.StringType))
                .withColumn("sentiment",
                        functions.col("sentiment").cast(DataTypes.StringType))
                 .withColumn(
                        "TweetCreatedHour",
                        functions.col("TweetCreatedHour").cast(
                                DataTypes.StringType))
                .withColumn("TweetCreatedMin",
                        functions.col("TweetCreatedMin").cast(DataTypes.StringType))
                .withColumn(
                        "TweetCreatedSec",
                        functions.col("TweetCreatedSec").cast(
                                DataTypes.IntegerType))
                .withColumn("UserCreatedMonth",
                        functions.col("UserCreatedMonth").cast(DataTypes.StringType))
                .withColumn("TweetCreatedSec",
                        functions.col("TweetCreatedSec").cast(DataTypes.StringType))
                .withColumn("hashtags",
                        functions.col("hashtags").cast(DataTypes.StringType))
                .withColumn("Name",
                        functions.col("Name").cast(DataTypes.StringType))
                .withColumn("Text",
                        functions.col("Text").cast(DataTypes.StringType));
        StreamingQuery query = dataAsSchema.writeStream().outputMode("append")
                .format("console").start();
        dataAsSchema
                .coalesce(1)
                .writeStream()
                .format("csv")
                // can be "orc", "json", "csv", "parquet" etc.
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(10))
                .option("truncate", false)
                .option("maxRecordsPerFile", 10000)
                .option("path",
                        "hdfs://localhost:8020/user/cloudera/twitterTweets")
                .option("checkpointLocation",
                        "hdfs://localhost:8020/user/cloudera/twitterTweetsCheckpoint") // args[1].toString()
                .start().awaitTermination();
        query.awaitTermination();
    }
}