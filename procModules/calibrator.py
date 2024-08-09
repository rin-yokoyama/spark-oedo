from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, arrays_zip, collect_list, DataFrame

def ToFloat(dataFrame: DataFrame, colName: str) -> DataFrame:

    float_df = dataFrame.withColumn(
        colName,
        F.expr("transform("+colName+", x -> x + cast(rand() as double))")
    )

    return float_df

def LinearCalib(dataFrame: DataFrame, idColName: str, valueColName: str) -> DataFrame:

    calib_df = dataFrame.withColumn(
        valueColName,
        F.expr("transform("+valueColName+", x -> x + cast(rand() as double))")
    )

    return calib_df