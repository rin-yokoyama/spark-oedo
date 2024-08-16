from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import mapper

def Tref(spark: "SparkSession", dataFrame: "F.DataFrame") -> F.DataFrame:
    mapdf = spark.read.csv(f"./map_files/tref.csv", header=True, inferSchema=True).cache()
    df_tref = mapper.Map(spark, dataFrame, "tref", mapdf).filter("edge == 1")
    # Filter out tref_edge == 0 and corresponding tref_id and tref_value elements
    #df_tref = df_tref.withColumn(
    #    "value",
    #    F.expr("filter(value, (x, i) -> edge[i] != 0)")
    #).withColumn(
    #    "id",
    #    F.expr("filter(id, (x, i) -> edge[i] != 0)")
    #).withColumn(
    #    "edge",
    #    F.expr("filter(edge, (x, i) -> x != 0)")
    #)
    
    return F.broadcast(df_tref)
