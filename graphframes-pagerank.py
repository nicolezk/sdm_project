from pyspark.sql import SparkSession
from graphframes import *
import time

# Configuration variables
host_port = 'hdfs://vulpix.fib.upc.es:27000'
hdfs_filepath_sdm = '/user/bdm/sdm/processed/'

# Initialize Spark Session
spark = SparkSession.builder \
    .master(f'local[*]') \
    .appName('bdm-proj') \
    .getOrCreate()

# Read the food web data data from HDFS
df_edges = spark.read \
    .option('header', 'true') \
    .option('sep', ';') \
    .csv(host_port + hdfs_filepath_sdm + 'food_web_edges.csv')

df_vertices = spark.read \
    .option('header', 'true') \
    .option('sep', ';') \
    .csv(host_port + hdfs_filepath_sdm + 'food_web_vertices.csv')

# Create the Spark GraphFrame
gf_food_web = GraphFrame(df_vertices, df_edges)

# Apply (And Time) PageRank until it converges with a tolerance of 0.0001
start_time = time.time()
results_df = gf_food_web.pageRank(tol=0.0001)
print("GraphFrame PageRank ran in --- %s seconds ---" % (time.time() - start_time))

# Write the results back to HDFS as a CSV file
results_df.vertices.coalesce(1).write.option('header', True).mode('overwrite').csv(host_port+hdfs_filepath_sdm+'foodweb_pagerank_graphframes.csv')