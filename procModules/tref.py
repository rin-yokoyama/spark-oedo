from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import mapper, constants

def Tref(spark: "SparkSession", dataFrame: "F.DataFrame") -> F.DataFrame:
    mapdf = spark.read.csv(f"hdfs://"+constants.CLUSTER_NAME+":9000"+constants.MAPFILE_PATH+"/tref.csv", header=True, inferSchema=True).cache()
    df_tref = mapper.Map(spark, dataFrame, "tref", mapdf).filter("edge == 1")
    
    return F.broadcast(df_tref)
