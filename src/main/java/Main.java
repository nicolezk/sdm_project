import FoodWebGraph.FoodWebGraph;
import com.google.common.io.Files;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class Main {

	static String HADOOP_COMMON_PATH = "SET THE ABSOLUTE PATH OF THE RESOURCE DIRECTORY WHERE THE WINUTILS IS LOCATED"; // "C:\\...\\SparkGraphXassignment\\src\\main\\resources"
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

		SparkConf conf = new SparkConf().setAppName("SparkGraphs_II").setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		ctx.setCheckpointDir(Files.createTempDir().getAbsolutePath());
		
		SQLContext sqlctx = new SQLContext(ctx);
		
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
                Level.ERROR);

		FoodWebGraph.computeFoodWeb(ctx, sqlctx);

	}

}
