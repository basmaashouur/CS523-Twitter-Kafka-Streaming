package cs523.BDTFinalProject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
	public static void main(String[] args) throws AnalysisException {

		//Logger.getLogger("org").setLevel(Level.OFF);
		
		final SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[*]");
        sparkConf.set("hive.metastore.uris", "thrift://localhost:9092");

        
        final SparkSession sparkSession = SparkSession.builder().appName("Spark SQL-Hive").config(sparkConf)
                .enableHiveSupport().getOrCreate();
        
        Dataset<Row> tabledata = sparkSession.sql("select * from TwitterData");
        tabledata.show();
        
        //tabledata = sparkSession.sql("select Source, sum(default.twitterdataanalytics.friendscount) as totalFriends, sum(default.twitterdataanalytics.favouritescount) as totalFavorites from TwitterDataAnalytics group by Source order by sum(default.twitterdataanalytics.favouritescount) desc ");
    
        //tabledata.show();
	}

}
