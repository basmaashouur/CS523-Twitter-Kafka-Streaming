package cs523.BDTFinalProject;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkSQL {
  public static void main(String[] args) throws AnalysisException {
    // Logger.getLogger("org").setLevel(Level.OFF);

    final SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]");
    sparkConf.set("hive.metastore.uris", "thrift://localhost:9083");

    final SparkSession sparkSession = SparkSession.builder()
      .appName("Spark SQL-Hive").config(sparkConf)
      .enableHiveSupport().getOrCreate();

    //create df
    Dataset < Row > df = sparkSession.sql("select hashtags from TwitterData where  hashtags  <> ''").toDF();
    Dataset < Row > df2 = sparkSession.sql("select tweethours from TwitterData where  hashtags  <> ''").toDF();

    List < String > listOne = df.as(Encoders.STRING()).collectAsList();
    List < String > listtwo = df2.as(Encoders.STRING()).collectAsList();
    
    int count  = 0;
    for (String element: listOne) {
      String[] array = element.split("\\|");

      for (int x = 0; x < array.length; x++) {
        Dataset < Row > recordsDF = sparkSession.createDataFrame(Arrays.asList(new hashtag(array[x], listtwo.get(count))), hashtag.class);

        recordsDF.write().mode(SaveMode.Append).insertInto("hashtags");
      }
      count++;
    }
    System.out.println(listOne);

  }
}