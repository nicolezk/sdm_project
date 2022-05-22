package FoodWebGraph;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.desc;

public class FoodWebGraph {
    public static void computeFoodWeb(JavaSparkContext ctx, SQLContext sqlCtx) {
        // Read the data and change column names
        Dataset<Row> wikiEdgesDF = sqlCtx.read().format("csv").option("header", "true").option("delimiter", ";").load("src/main/resources/food_web_edges.csv");
        wikiEdgesDF = wikiEdgesDF.withColumnRenamed("_c0", "src").withColumnRenamed("_c1","dst");

        Dataset<Row> wikiVerticesDF = sqlCtx.read().format("csv").option("header", "true").option("delimiter", ";").load("src/main/resources/food_web_vertices.csv");
        wikiVerticesDF = wikiVerticesDF.withColumnRenamed("_c1", "id").withColumnRenamed("_c0","name");

        // Create the Spark GraphFrame
        GraphFrame gf = GraphFrame.apply(wikiVerticesDF,wikiEdgesDF);

		gf.edges().show();
		gf.vertices().show();


        gf.pageRank()
                .maxIter(20)
                .resetProbability(0.3)
                .run()
                .vertices()
                .sort(desc("pagerank"))
                .show(10);
    }
}
